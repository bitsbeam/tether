# frozen_string_literal: true

RSpec.describe Tether::JetStream::Schema do
  let(:part) do
    Class.new(described_class) do
      field :label, :string
      field :size, :integer, key: "sz"
    end
  end

  let(:schema) do
    nested = part
    Class.new(described_class) do
      field :name, :string
      field :age, :duration, key: "max_age"
      field :waits, [:duration]
      field :created, :time
      field :mode, :enum, values: { fast: "fast", work_queue: "workqueue" }, level: { fast: 3 }
      field :part, nested
      field :parts, [nested]
      field :flag, :boolean, level: 2
      field :ttl, :duration, level: 1
      field :metadata, :object
    end
  end

  def named(klass, name)
    klass.tap { |anonymous| allow(anonymous).to receive(:name).and_return(name) }
  end

  describe ".field" do
    it "defines a reader for the field" do
      expect(schema.new(name: "ORDERS").name).to eq("ORDERS")
    end

    it "records the JSON key, type and level of the field" do
      expect(schema.fields[:age]).to have_attributes(key: "max_age", type: :duration, level: 0)
    end

    it "defaults the JSON key to the field's name" do
      expect(schema.fields[:name].key).to eq("name")
    end

    it "gives a subclass the fields of its parent without adding its own to the parent" do
      child = Class.new(schema) { field :extra, :string }

      expect(child.fields.keys).to include(:name, :extra)
      expect(schema.fields.keys).not_to include(:extra)
    end
  end

  describe ".fields" do
    it "lists the declared fields by their Ruby names in declaration order" do
      expect(part.fields.keys).to eq(%i[label size])
    end
  end

  describe ".from_h" do
    it "reads fields from their JSON keys" do
      expect(part.from_h("label" => "a", "sz" => 3)).to have_attributes(label: "a", size: 3)
    end

    it "converts nanoseconds to whole seconds" do
      expect(schema.from_h("max_age" => 86_400_000_000_000).age).to eql(86_400)
    end

    it "converts nanoseconds to fractional seconds when they are not whole" do
      expect(schema.from_h("max_age" => 1_500_000_000).age).to eql(1.5)
    end

    it "converts each element of an array of durations" do
      expect(schema.from_h("waits" => [1_000_000_000, 250_000_000]).waits).to eq([1, 0.25])
    end

    it "converts an RFC 3339 timestamp to a Time with its nanoseconds" do
      created = schema.from_h("created" => "2026-11-22T10:15:30.123456789Z").created

      expect(created).to eq(Time.utc(2026, 11, 22, 10, 15, 30, 123_456.789r))
    end

    it "reads the zero time the server sends for an unset timestamp as nil" do
      expect(schema.from_h("created" => "0001-01-01T00:00:00Z").created).to be_nil
    end

    it "converts an enumeration string to its symbol" do
      expect(schema.from_h("mode" => "workqueue").mode).to eq(:work_queue)
    end

    it "turns an enumeration string it does not know into a symbol" do
      expect(schema.from_h("mode" => "brand_new").mode).to eq(:brand_new)
    end

    it "builds a nested structure with its class" do
      expect(schema.from_h("part" => { "label" => "a", "sz" => 1 }).part).to eq(part.new(label: "a", size: 1))
    end

    it "builds each element of an array of nested structures" do
      expect(schema.from_h("parts" => [{ "sz" => 1 }, { "sz" => 2 }]).parts.map(&:size)).to eq([1, 2])
    end

    it "leaves a field the hash does not carry unset" do
      expect(schema.from_h("name" => "a").age).to be_nil
    end

    it "keeps the hash it was built from, unknown keys included" do
      raw = { "name" => "a", "from_the_future" => 1 }

      expect(schema.from_h(raw).raw).to eq(raw)
    end
  end

  describe ".coerce" do
    it "returns an instance of the class untouched" do
      instance = part.new(label: "a")

      expect(part.coerce(instance)).to be(instance)
    end

    it "builds an instance from a hash of keywords" do
      expect(part.coerce({ label: "a" })).to eq(part.new(label: "a"))
    end
  end

  describe ".new" do
    it "rejects a keyword that is not a declared field" do
      expect { named(part, "Tether::JetStream::Part").new(labl: "a") }
        .to raise_error(ArgumentError, "unknown Part field: labl")
    end

    it "rejects a symbol that is not a value of the enumeration" do
      expect { schema.new(mode: :slow) }.to raise_error(ArgumentError, "mode: :slow is not one of :fast, :work_queue")
    end

    it "turns a hash given for a nested structure into its class" do
      expect(schema.new(part: { label: "a" }).part).to eq(part.new(label: "a"))
    end

    it "turns hashes in an array of nested structures into their class" do
      expect(schema.new(parts: [{ size: 1 }, part.new(size: 2)]).parts).to eq([part.new(size: 1), part.new(size: 2)])
    end

    it "rejects an unknown keyword inside a nested structure" do
      expect { schema.new(part: { labl: "a" }) }.to raise_error(ArgumentError, /field: labl/)
    end

    it "returns a frozen object" do
      expect(schema.new(name: "a")).to be_frozen
    end

    it "starts with an empty raw hash" do
      expect(schema.new(name: "a").raw).to eq({})
    end
  end

  describe "#[]" do
    it "reads a field by its symbol" do
      expect(part.new(size: 3)[:size]).to eq(3)
    end

    it "reads a field by its name as a string" do
      expect(part.new(size: 3)["size"]).to eq(3)
    end
  end

  describe "#to_h" do
    it "emits only the fields that are set, under their JSON keys" do
      expect(part.new(size: 3).to_h).to eq("sz" => 3)
    end

    it "leaves out a field set to nil" do
      expect(part.new(label: nil, size: 3).to_h).to eq("sz" => 3)
    end

    it "keeps a field set to false or zero" do
      expect(schema.new(flag: false, age: 0).to_h).to eq("flag" => false, "max_age" => 0)
    end

    it "converts seconds to integer nanoseconds" do
      expect(schema.new(age: 86_400).to_h).to eq("max_age" => 86_400_000_000_000)
    end

    it "converts fractional seconds to integer nanoseconds" do
      expect(schema.new(age: 0.1).to_h["max_age"]).to eql(100_000_000)
    end

    it "converts each element of an array of durations" do
      expect(schema.new(waits: [1, 0.5]).to_h).to eq("waits" => [1_000_000_000, 500_000_000])
    end

    it "converts a Time to RFC 3339 in UTC with nanoseconds" do
      created = Time.new(2026, 11, 22, 12, 15, 30.5r, "+02:00")

      expect(schema.new(created: created).to_h).to eq("created" => "2026-11-22T10:15:30.500000000Z")
    end

    it "leaves the Time it was given in its own zone" do
      created = Time.new(2026, 11, 22, 12, 15, 30, "+02:00")
      schema.new(created: created).to_h

      expect(created.utc_offset).to eq(7200)
    end

    it "converts an enumeration symbol to its wire string" do
      expect(schema.new(mode: :work_queue).to_h).to eq("mode" => "workqueue")
    end

    it "passes an enumeration value given as a string through" do
      expect(schema.new(mode: "brand_new").to_h).to eq("mode" => "brand_new")
    end

    it "writes an enumeration string it does not know back as the server sent it" do
      expect(schema.from_h("mode" => "brand_new").with(name: "a").to_h).to eq("mode" => "brand_new", "name" => "a")
    end

    it "converts nested structures to their wire form" do
      wire = schema.new(part: { size: 1 }, parts: [{ label: "b" }]).to_h

      expect(wire).to eq("part" => { "sz" => 1 }, "parts" => [{ "label" => "b" }])
    end

    it "carries keys it does not know through from the hash it was built from" do
      expect(schema.from_h("name" => "a", "from_the_future" => { "x" => 1 }).to_h)
        .to eq("name" => "a", "from_the_future" => { "x" => 1 })
    end

    it "round trips a decoded hash unchanged" do
      raw = { "name" => "a", "max_age" => 1_500_000_000, "mode" => "fast", "part" => { "label" => "x", "sz" => 2 },
              "created" => "2026-11-22T10:15:30.123456789Z" }

      expect(schema.from_h(raw).to_h).to eq(raw)
    end

    it "drops the metadata keys the server owns" do
      wire = schema.new(metadata: { "owner" => "me", "_nats.level" => "5", "_nats.req.level" => "0" }).to_h

      expect(wire).to eq("metadata" => { "owner" => "me" })
    end

    it "drops metadata that holds nothing but the keys the server owns" do
      expect(schema.from_h("name" => "a", "metadata" => { "_nats.ver" => "2.15.3" }).to_h).to eq("name" => "a")
    end
  end

  describe "#to_json" do
    it "encodes the wire form as JSON" do
      expect(JSON.parse(schema.new(name: "a", age: 2).to_json)).to eq("name" => "a", "max_age" => 2_000_000_000)
    end
  end

  describe "#with" do
    it "returns a copy with the given fields replaced" do
      original = part.new(label: "a", size: 1)

      expect(original.with(size: 2)).to eq(part.new(label: "a", size: 2))
      expect(original.size).to eq(1)
    end

    it "removes a field set to nil" do
      expect(part.new(label: "a", size: 1).with(label: nil).to_h).to eq("sz" => 1)
    end

    it "removes a field that came from the server when it is set to nil" do
      expect(part.from_h("label" => "a", "sz" => 1).with(label: nil).to_h).to eq("sz" => 1)
    end

    it "carries keys it does not know through to the copy" do
      copy = schema.from_h("name" => "a", "from_the_future" => true).with(name: "b")

      expect(copy.to_h).to eq("name" => "b", "from_the_future" => true)
    end

    it "rejects a keyword that is not a declared field" do
      expect { part.new(size: 1).with(sise: 2) }.to raise_error(ArgumentError, /field: sise/)
    end
  end

  describe "#==" do
    it "equals an object of the same class with the same wire form" do
      expect(part.new(label: "a")).to eq(part.from_h("label" => "a"))
    end

    it "differs from an object with another value" do
      expect(part.new(label: "a")).not_to eq(part.new(label: "b"))
    end

    it "differs from an object of another class with the same wire form" do
      other = Class.new(described_class) { field :label, :string }

      expect(part.new(label: "a")).not_to eq(other.new(label: "a"))
    end

    it "ignores the metadata keys the server owns" do
      desired = schema.new(name: "a", metadata: { "owner" => "me" })
      stored = schema.from_h("name" => "a", "metadata" => { "owner" => "me", "_nats.req.level" => "0" })

      expect(stored).to eq(desired)
    end

    it "treats metadata that holds only server keys as no metadata" do
      expect(schema.from_h("name" => "a", "metadata" => { "_nats.level" => "5" })).to eq(schema.new(name: "a"))
    end
  end

  describe "#hash" do
    it "is the same for equal objects, so they are one hash key" do
      expect({ part.new(label: "a") => 1 }[part.from_h("label" => "a")]).to eq(1)
    end
  end

  describe "#api_level" do
    it "is 0 when no field that needs a level is set" do
      expect(schema.new(name: "a", age: 5).api_level).to eq(0)
    end

    it "is the level of a field that is set" do
      expect(schema.new(ttl: 5).api_level).to eq(1)
    end

    it "is the highest level among the fields that are set" do
      expect(schema.new(ttl: 5, flag: true).api_level).to eq(2)
    end

    it "ignores a field that is false" do
      expect(schema.new(flag: false).api_level).to eq(0)
    end

    it "ignores a field that is zero" do
      expect(schema.new(ttl: 0).api_level).to eq(0)
    end

    it "ignores a field that is nil" do
      expect(schema.new(flag: nil).api_level).to eq(0)
    end

    it "uses the level of the value when only some values need one" do
      expect(schema.new(mode: :fast).api_level).to eq(3)
    end

    it "is 0 for a value that needs no level" do
      expect(schema.new(mode: :work_queue).api_level).to eq(0)
    end
  end

  describe "#inspect" do
    it "shows the class and the fields that are set" do
      expect(named(part, "Part").new(label: "a").inspect).to eq('#<Part label="a">')
    end
  end
end
