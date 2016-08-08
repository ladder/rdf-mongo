module RDF
  module Mongo
    class Repository < ::RDF::Repository
      # The Mongo database instance
      # @return [Mongo::DB]
      attr_reader :client

      # The collection used for storing quads
      # @return [Mongo::Collection]
      attr_reader :collection

      ##
      # Initializes this repository instance.
      #
      # @overload initialize(options = {}, &block)
      #   @param  [Hash{Symbol => Object}] options
      #   @option options [String, #to_s] :title (nil)
      #   @option options [URI, #to_s]    :uri (nil)
      #     URI in the form `mongodb://host:port/db`. The URI should also identify the collection use, but appending a `collection` path component such as `mongodb://host:port/db/collection`, this ensures that the collection will be maintained if cloned. See [Mongo::Client options](https://docs.mongodb.org/ecosystem/tutorial/ruby-driver-tutorial-2-0/#uri-options-conversions) for more information on Mongo URIs.
      #
      # @overload initialize(options = {}, &block)
      #   @param  [Hash{Symbol => Object}] options
      #     See [Mongo::Client options](https://docs.mongodb.org/ecosystem/tutorial/ruby-driver-tutorial-2-0/#uri-options-conversions) for more information on Mongo Client options.
      #   @option options [String, #to_s] :title (nil)
      #   @option options [String] :host
      #     a single address or an array of addresses, which may contain a port designation
      #   @option options [Integer] :port (27017) applied to host address(es)
      #   @option options [String] :database ('quadb')
      #   @option options [String] :collection ('quads')
      #
      # @yield  [repository]
      # @yieldparam [Repository] repository
      def initialize(options = {}, &block)
        collection = nil
        if options[:uri]
          options = options.dup
          uri = RDF::URI(options.delete(:uri))
          _, db, coll = uri.path.split('/')
          collection = coll || options.delete(:collection)
          db ||= "quadb"
          uri.path = "/#{db}" if coll
          @client = ::Mongo::Client.new(uri.to_s, options)
        else
          warn "[DEPRECATION] RDF::Mongo::Repository#initialize expects a uri argument. Called from #{Gem.location_of_caller.join(':')}" unless options.empty?
          options[:database] ||= options.delete(:db) # 1.x compat
          options[:database] ||= 'quadb'
          hosts = Array(options[:host] || 'localhost')
          hosts.map! {|h| "#{h}:#{options[:port]}"} if options[:port]
          @client = ::Mongo::Client.new(hosts, options)
        end

        @collection = @client[options.delete(:collection) || 'quads']
        @collection.indexes.create_many([
          {key: {s: "hashed"}},
          {key: {p: "hashed"}},
          {key: {o: "hashed"}},
          {key: {g: "hashed"}},
          {key: {s: 1, p: 1, o: 1, g: 1}},
          {key: {st: 1, pt: 1, ot: 1, gt: 1}},
          {key: {sl: 1, pl: 1, ol: 1, gl: 1}},
        ])
        super(options, &block)
      end

      # @see RDF::Mutable#insert_statement
      def supports?(feature)
        case feature.to_sym
          when :graph_name   then true
          when :atomic_write then true
          when :validity     then @options.fetch(:with_validity, true)
          else false
        end
      end

      def apply_changeset(changeset)
        ops = []

        changeset.deletes.each do |d|
          st_mongo = statement_to_mongo(d)
          ops << { delete_one: { filter: st_mongo } }
        end

        changeset.inserts.each do |i|
          st_mongo = statement_to_mongo(i)
          ops << { update_one: { filter: st_mongo, update: st_mongo, upsert: true } }
        end

        # Only use an ordered write if we have both deletes and inserts
        ordered = ! (changeset.inserts.empty? or changeset.deletes.empty?)
        @collection.bulk_write(ops, ordered: ordered)
      end

      def insert_statement(statement)
        st_mongo = statement_to_mongo(statement)
        @collection.update_one(st_mongo, st_mongo, upsert: true)
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        st_mongo = statement_to_mongo(statement)
        @collection.delete_one(st_mongo)
      end

      ##
      # @private
      # @see RDF::Durable#durable?
      def durable?; true; end

      ##
      # @private
      # @see RDF::Countable#empty?
      def empty?; @collection.count == 0; end

      ##
      # @private
      # @see RDF::Countable#count
      def count
        @collection.count
      end

      def clear_statements
        @collection.delete_many
      end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        @collection.find(statement_to_mongo(statement)).count > 0
      end
      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        if block_given?
          @collection.find().each do |document|
            block.call(RDF::Mongo::Conversion.statement_from_mongo(document))
          end
        end
        enum_statement
      end
      alias_method :each, :each_statement

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        @collection.find(RDF::Mongo::Conversion.p_to_mongo(:graph_name, value)).count > 0
      end

      protected

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?

        # A pattern graph_name of `false` is used to indicate the default graph
        pm = RDF::Mongo::Conversion.pattern_to_mongo(pattern)

        @collection.find(pm).each do |document|
          block.call(RDF::Mongo::Conversion.statement_from_mongo(document))
        end
      end

      private

        def enumerator! # @private
          require 'enumerator' unless defined?(::Enumerable)
          @@enumerator_klass = defined?(::Enumerable::Enumerator) ? ::Enumerable::Enumerator : ::Enumerator
        end

        def statement_to_mongo(statement)
          raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
          RDF::Mongo::Conversion.statement_to_mongo(statement)
        end
    end
  end
end