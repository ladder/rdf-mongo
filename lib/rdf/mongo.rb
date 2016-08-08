require 'rdf'
require 'enumerator'
require 'mongo'

module RDF
  class Statement
    ##
    # Creates a BSON representation of the statement.
    # @return [Hash]
    def to_mongo
      self.to_hash.inject({}) do |hash, (place_in_statement, entity)|
        hash.merge(RDF::Mongo::Conversion.to_mongo(entity, place_in_statement))
      end
    end

    ##
    # Create BSON for a statement representation. Note that if the statement has no graph name,
    # a value of `false` will be used to indicate the default context
    #
    # @param [RDF::Statement] statement
    # @return [Hash] Generated BSON representation of statement.
    def self.from_mongo(statement)
      RDF::Statement.new(
        subject:    RDF::Mongo::Conversion.from_mongo(statement['subject'],   statement['s_type'], statement['s_literal']),
        predicate:  RDF::Mongo::Conversion.from_mongo(statement['predicate'], statement['p_type'], statement['p_literal']),
        object:     RDF::Mongo::Conversion.from_mongo(statement['object'],    statement['o_type'], statement['o_literal']),
        graph_name: RDF::Mongo::Conversion.from_mongo(statement['context'],   statement['c_type'], statement['c_literal']))
    end
  end

  module Mongo
    autoload :VERSION, "rdf/mongo/version"

    class Conversion
      ##
      # Translate an RDF::Value type to BSON key/value pairs.
      #
      # @param [RDF::Value, Symbol, false, nil] value
      #   URI, BNode or Literal. May also be a Variable or Symbol to indicate
      #   a pattern for a named graph, or `false` to indicate the default graph.
      #   A value of `nil` indicates a pattern that matches any value.
      # @param [:subject, :predicate, :object, :graph_name] place_in_statement
      #   Position within statement.
      # @return [Hash] BSON representation of the statement
      def self.to_mongo(value, place_in_statement)
        case place_in_statement
        when :subject
          value_type, position, literal_extra = :s_type, :subject, :s_literal
        when :predicate
          value_type, position, literal_extra = :p_type, :predicate, :p_literal
        when :object
          value_type, position, literal_extra = :o_type, :object, :o_literal
        when :graph_name
          value_type, position, literal_extra = :c_type, :context, :c_literal
        end

        case value
        when RDF::URI
          pos, type = value.to_s, :uri
        when RDF::Node
          pos, type = value.id.to_s, :node
        when RDF::Literal
          if value.has_language?
            pos, type, ll = value.value, :literal_lang, value.language.to_s
          elsif value.has_datatype?
            pos, type, ll = value.value, :literal_type, value.datatype.to_s
          else
            pos, type, ll = value.value, :literal, nil
          end

        # ===== query patterns for named graphs
        when RDF::Query::Variable, Symbol
          # Returns anything other than the default context
          pos, type = nil, {"$ne" => :default}
        when false
          # Used for the default context
          pos, type = false, :default
        # =====
        end
=begin
        when nil # FIXME: shouldn't return anything
          pos, type = nil, nil
        else # FIXME: we should never get here
          pos, type = value.to_s, :uri
        end
        pos = nil if pos == ''
        # =====
=end

        h = { position => pos, value_type => type, literal_extra => ll }
        h.delete_if {|kk,_| h[kk].nil?}
      end

      ##
      # Translate an BSON positional reference to an RDF Value.
      #
      # @return [RDF::Value]
      def self.from_mongo(value, type = :uri, literal_extra = nil)
        case type
        when :uri
          RDF::URI.intern(value)
        when :literal_lang
          RDF::Literal.new(value, language: literal_extra.to_sym)
        when :literal_type
          RDF::Literal.new(value, datatype: RDF::URI.intern(literal_extra))
        when :literal
          RDF::Literal.new(value)
        when :node
          @nodes ||= {}
          @nodes[value] ||= RDF::Node.new(value)
        when :default
          nil # The default context returns as nil, although it's queried as false.
        end
      end
    end

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
          {key: {statement: 1}},
          {key: {predicate: 1}},
          {key: {object: "hashed"}},
          {key: {context: 1}},
          {key: {statement: 1, predicate: 1}},
          #{key: {s: 1, o: "hashed"}}, # Muti-key hashed indexes not allowed
          #{key: {p: 1, o: "hashed"}}, # Muti-key hashed indexes not allowed
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
          ops << { delete_one: { filter: statement_to_delete(d)} }
        end

        changeset.inserts.each do |i|
          ops << { update_one: { filter: statement_to_insert(i), update: statement_to_insert(i), upsert: true} }
        end

        # Only use an ordered write if we have both deletes and inserts
        ordered = ! (changeset.inserts.empty? or changeset.deletes.empty?)
        @collection.bulk_write(ops, ordered: ordered)
      end

      def insert_statement(statement)
        st_mongo = statement_to_insert(statement)
        @collection.update_one(st_mongo, st_mongo, upsert: true)
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        st_mongo = statement_to_delete(statement)
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
        @collection.find(statement.to_mongo).count > 0
      end
      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        @nodes = {} # reset cache. FIXME this should probably be in Node.intern
        if block_given?
          @collection.find().each do |document|
            block.call(RDF::Statement.from_mongo(document))
          end
        end
        enum_statement
      end
      alias_method :each, :each_statement

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        @collection.find(RDF::Mongo::Conversion.to_mongo(value, :graph_name)).count > 0
      end

      protected

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?
        @nodes = {} # reset cache. FIXME this should probably be in Node.intern

        # A pattern graph_name of `false` is used to indicate the default graph
        pm = pattern.to_mongo
        pm.merge!(context: nil, c_type: :default) if pattern.graph_name == false
        #puts "query using #{pm.inspect}"
        @collection.find(pm).each do |document|
          block.call(RDF::Statement.from_mongo(document))
        end
      end

      private

        def enumerator! # @private
          require 'enumerator' unless defined?(::Enumerable)
          @@enumerator_klass = defined?(::Enumerable::Enumerator) ? ::Enumerable::Enumerator : ::Enumerator
        end

        def statement_to_insert(statement)
          raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
          st_mongo = statement.to_mongo
          st_mongo[:c_type] ||= :default # Indicate statement is in the default graph
          #puts "insert statement: #{st_mongo.inspect}"
          st_mongo
        end

        def statement_to_delete(statement)
          st_mongo = statement.to_mongo
          st_mongo[:c_type] = :default if statement.graph_name.nil?
          st_mongo
        end
    end
  end
end
