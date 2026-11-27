# frozen_string_literal: true

require "openssl"

module Tether
  module Connection
    # Builds the TLS context a connection uses from the `tls:` options.
    module TLS
      # What the TLS handshake of a connection settled on.
      #
      # @!attribute [r] version
      #   @return [String] the protocol version, such as "TLSv1.3"
      # @!attribute [r] cipher
      #   @return [String] the name of the cipher suite
      # @!attribute [r] peer_certificate
      #   @return [OpenSSL::X509::Certificate, nil] the server's certificate
      # @!attribute [r] peer_certificate_chain
      #   @return [Array<OpenSSL::X509::Certificate>] the chain the server sent, its own certificate first
      State = Data.define(:version, :cipher, :peer_certificate, :peer_certificate_chain) do
        # @param socket [OpenSSL::SSL::SSLSocket] a socket after its handshake
        # @return [State] the state of its session
        def self.of(socket)
          new(version: socket.ssl_version, cipher: socket.cipher&.first, peer_certificate: socket.peer_cert,
              peer_certificate_chain: socket.peer_cert_chain || [])
        end
      end

      # @param settings [Hash{Symbol => Object}] the validated `tls:` options
      # @return [OpenSSL::SSL::SSLContext] a context ready for a client handshake
      # @raise [TLSError] if a certificate or key cannot be loaded
      def self.context(settings)
        return settings[:context] if settings[:context]

        context = OpenSSL::SSL::SSLContext.new
        context.verify_mode = settings[:verify] ? OpenSSL::SSL::VERIFY_PEER : OpenSSL::SSL::VERIFY_NONE
        context.min_version = settings[:min_version] if settings[:min_version]
        context.cert_store = store(settings)
        identity(context, settings)
        context
      rescue OpenSSL::OpenSSLError, SystemCallError => e
        raise TLSError, "could not build the TLS context: #{e.message}"
      end

      def self.store(settings)
        store = OpenSSL::X509::Store.new
        store.set_default_paths
        store.add_file(settings[:ca_file]) if settings[:ca_file]
        store.add_path(settings[:ca_path]) if settings[:ca_path]
        store
      end
      private_class_method :store

      def self.identity(context, settings)
        return unless settings[:cert_file]

        context.cert = OpenSSL::X509::Certificate.new(File.read(settings[:cert_file]))
        context.key = OpenSSL::PKey.read(File.read(settings[:key_file] || settings[:cert_file]))
      end
      private_class_method :identity
    end
  end
end
