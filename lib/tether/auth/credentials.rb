# frozen_string_literal: true

module Tether
  class Auth
    # Reads the `.creds` format: a user JWT and an NKEY seed, each between
    # dashed BEGIN and END lines.
    module Credentials
      BLOCK = /-{3,}BEGIN [^-]+-{3,}\r?\n(.+?)\r?\n-{3,}END [^-]+-{3,}/m

      # @param text [String] the contents of a credentials file
      # @return [Array(String, String)] the JWT and the seed
      # @raise [CredentialsError] if either block is missing
      def self.parse(text)
        jwt, seed = text.to_s.scan(BLOCK).map { |match| match.first.strip }
        raise CredentialsError, "credentials must hold a JWT and an NKEY seed" unless jwt && seed

        [jwt, seed]
      end
    end
  end
end
