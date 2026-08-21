# frozen_string_literal: true

RSpec.describe Tether::Service::Group do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:service) { nc.services.add(name: "calc", version: "1.2.0") }

  after { nc.close }

  describe "#prefix" do
    it "returns the name of a top-level group" do
      expect(service.group("math").prefix).to eq("math")
    end

    it "returns the joined names of nested groups" do
      expect(service.group("math").group("v1.int").prefix).to eq("math.v1.int")
    end

    it "returns an empty string for a group with an empty name" do
      expect(service.group("").prefix).to eq("")
    end
  end

  describe "#queue_group" do
    it "returns the service's queue group when none is given" do
      expect(service.group("math").queue_group).to eq("q")
    end

    it "returns the queue group given" do
      expect(service.group("math", queue: "math-q").queue_group).to eq("math-q")
    end

    it "returns nil when the queue is false" do
      expect(service.group("math", queue: false).queue_group).to be_nil
    end
  end

  describe "#endpoint" do
    it "serves requests on the prefix joined with the endpoint name" do
      service.group("math").endpoint("sqrt") { |request| request.respond("7") }

      expect(nc.request("math.sqrt").data).to eq("7")
    end

    it "serves requests on the prefix joined with the subject given" do
      endpoint = service.group("math").endpoint("sqrt", subject: "root.square") { |request| request.respond("7") }

      expect(nc.request("math.root.square").data).to eq("7")
      expect(endpoint.subject).to eq("math.root.square")
    end

    it "adds no prefix and no dot under a group with an empty name" do
      expect(service.group("").endpoint("sqrt") { nil }.subject).to eq("sqrt")
    end

    it "accepts a wildcard subject ending in >" do
      service.group("math").endpoint("any", subject: "ops.>") { |request| request.respond(request.subject) }

      expect(nc.request("math.ops.add.int").data).to eq("math.ops.add.int")
    end

    it "uses the group's queue group" do
      expect(service.group("math", queue: "math-q").endpoint("sqrt") { nil }.queue_group).to eq("math-q")
    end

    it "uses the queue group given over the group's" do
      endpoint = service.group("math", queue: "math-q").endpoint("sqrt", queue: "own") { nil }

      expect(endpoint.queue_group).to eq("own")
    end

    it "uses no queue group under a group without one" do
      expect(service.group("math", queue: false).endpoint("sqrt") { nil }.queue_group).to be_nil
    end

    it "uses the queue group given under a group without one" do
      endpoint = service.group("math", queue: false).endpoint("sqrt", queue: "own") { nil }

      expect(endpoint.queue_group).to eq("own")
    end

    it "uses no queue group when the queue is false under a group with one" do
      endpoint = service.group("math", queue: "math-q").endpoint("sqrt", queue: false) { nil }

      expect(endpoint.queue_group).to be_nil
    end

    it "lists the endpoint in the service's info" do
      service.group("math").endpoint("sqrt") { nil }

      expect(service.info.endpoints.map(&:subject)).to eq(["math.sqrt"])
    end

    it "raises on an invalid endpoint name" do
      expect { service.group("math").endpoint("a b") { nil } }.to raise_error(ArgumentError, /invalid endpoint name/)
    end
  end

  describe "#group" do
    it "serves the endpoints of a nested group under both names" do
      service.group("math").group("v1").endpoint("sqrt") { |request| request.respond("7") }

      expect(nc.request("math.v1.sqrt").data).to eq("7")
    end

    it "skips an empty name when joining" do
      expect(service.group("math").group("").group("v1").prefix).to eq("math.v1")
    end

    it "inherits the queue group of the group above" do
      expect(service.group("math", queue: "math-q").group("v1").queue_group).to eq("math-q")
    end

    it "inherits no queue group from a group without one" do
      expect(service.group("math", queue: false).group("v1").queue_group).to be_nil
    end

    it "uses the queue group given over the one above" do
      expect(service.group("math", queue: false).group("v1", queue: "v1-q").queue_group).to eq("v1-q")
    end

    it "raises on a name with >" do
      expect { service.group("math").group(">") }.to raise_error(Tether::InvalidSubjectError, /group name/)
    end

    it "raises on a name with a space" do
      expect { service.group("math").group("v 1") }.to raise_error(Tether::InvalidSubjectError)
    end
  end
end
