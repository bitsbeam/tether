# frozen_string_literal: true

RSpec.describe Tether::Protocol::Subject do
  describe ".check!" do
    it "returns a safe value" do
      expect(described_class.check!("orders.new")).to eq("orders.new")
    end

    it "accepts wildcards and empty tokens, which cannot corrupt the stream" do
      expect(described_class.check!("a..*.>")).to eq("a..*.>")
    end

    it "raises for an empty value" do
      expect { described_class.check!("") }.to raise_error(Tether::InvalidSubjectError, /invalid subject/)
    end

    it "raises for nil" do
      expect { described_class.check!(nil) }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for a space" do
      expect { described_class.check!("a b") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for CRLF, which would start another protocol line" do
      expect { described_class.check!("a\r\nPUB x 0") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for a NUL byte" do
      expect { described_class.check!("a\0b") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "names the kind of value in the message" do
      expect { described_class.check!("a b", "queue group") }
        .to raise_error(Tether::InvalidSubjectError, /invalid queue group: "a b"/)
    end
  end

  describe ".literal!" do
    it "returns a literal subject" do
      expect(described_class.literal!("orders.new.eu")).to eq("orders.new.eu")
    end

    it "accepts a token that merely contains a wildcard character" do
      expect(described_class.literal!("a.b*.c>")).to eq("a.b*.c>")
    end

    it "raises for a * token" do
      expect { described_class.literal!("a.*.c") }.to raise_error(Tether::InvalidSubjectError, /not a literal subject/)
    end

    it "raises for a > token" do
      expect { described_class.literal!("a.>") }.to raise_error(Tether::InvalidSubjectError, /not a literal subject/)
    end

    it "raises for a lone wildcard" do
      expect { described_class.literal!("*") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for a leading dot" do
      expect { described_class.literal!(".a") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for a trailing dot" do
      expect { described_class.literal!("a.") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for an empty token in the middle" do
      expect { described_class.literal!("a..b") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises for whitespace with the unsafe value message" do
      expect { described_class.literal!("a b") }.to raise_error(Tether::InvalidSubjectError, /invalid subject/)
    end

    it "raises for a value that is not a string" do
      expect { described_class.literal!(nil) }.to raise_error(Tether::InvalidSubjectError)
    end
  end

  describe ".pattern!" do
    it "returns a subject with wildcards" do
      expect(described_class.pattern!("orders.*.>")).to eq("orders.*.>")
    end

    it "raises for an empty token" do
      expect { described_class.pattern!("a..b") }.to raise_error(Tether::InvalidSubjectError, /empty token/)
    end

    it "raises for a trailing dot" do
      expect { described_class.pattern!("a.") }.to raise_error(Tether::InvalidSubjectError, /empty token/)
    end

    it "raises for whitespace" do
      expect { described_class.pattern!("a b") }.to raise_error(Tether::InvalidSubjectError, /invalid subject/)
    end
  end

  describe ".match?" do
    it "matches an identical literal" do
      expect(described_class.match?("a.b", "a.b")).to be(true)
    end

    it "matches * against exactly one token" do
      expect(described_class.match?("a.*.c", "a.b.c")).to be(true)
    end

    it "rejects * against a missing token" do
      expect(described_class.match?("a.*", "a")).to be(false)
    end

    it "matches > against one or more trailing tokens" do
      expect(described_class.match?("a.>", "a.b.c")).to be(true)
    end

    it "rejects > against no trailing token" do
      expect(described_class.match?("a.>", "a")).to be(false)
    end

    it "rejects a subject longer than the pattern" do
      expect(described_class.match?("a.b", "a.b.c")).to be(false)
    end

    it "rejects a different literal token" do
      expect(described_class.match?("a.b", "a.c")).to be(false)
    end
  end
end
