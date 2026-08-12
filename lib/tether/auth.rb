# frozen_string_literal: true

require "tether/auth/nkey"
require "tether/auth/credentials"

module Tether
  # Turns the authentication options into the fields CONNECT carries.
  #
  # Files and callables are read on every connect, so rotated credentials are
  # picked up by the next reconnect.
  class Auth
    # @param options [Options] the client's options
    def initialize(options)
      @options = options
    end

    # @param uri [URI::Generic] the server being connected to, whose userinfo wins for that server
    # @param nonce [String, nil] the nonce from the server's INFO
    # @return [Hash{Symbol => String}] the authentication fields for CONNECT
    # @raise [CredentialsError] if a credential cannot be read or decoded
    def fields(uri, nonce)
      return from_url(uri) if uri.user
      return @options.auth.call(nonce).to_h.transform_keys(&:to_sym) if @options.auth

      jwt, seed = jwt_and_seed
      if jwt then signed({ jwt: jwt }, seed, nonce)
      elsif seed then signed({ nkey: NKey.new(seed).public_key }, seed, nonce)
      else simple
      end
    end

    private

    def from_url(uri)
      user = URI.decode_www_form_component(uri.user)
      return { auth_token: user } unless uri.password

      { user: user, pass: URI.decode_www_form_component(uri.password) }
    end

    def simple
      token = resolve(@options.token)
      return { auth_token: token } if token

      user = resolve(@options.user)
      user ? { user: user, pass: resolve(@options.password).to_s } : {}
    end

    def jwt_and_seed
      text = @options.credentials || read(@options.credentials_file)
      return Credentials.parse(resolve(text)) if text

      [resolve(@options.jwt), resolve(@options.nkey_seed) || read(@options.nkey_seed_file)]
    end

    def signed(fields, seed, nonce)
      return fields unless seed && nonce

      fields.merge(sig: NKey.new(seed).sign(nonce))
    end

    def resolve(value)
      value.respond_to?(:call) ? value.call : value
    end

    def read(path)
      path && File.read(path)
    rescue SystemCallError => e
      raise CredentialsError, "could not read #{path}: #{e.message}"
    end
  end
end
