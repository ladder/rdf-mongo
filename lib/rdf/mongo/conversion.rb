require 'pry'
module RDF
  module Mongo
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
      def self.entity_to_mongo(entity, place_in_statement)
        case place_in_statement
        when :subject
          value_type, literal_extra = :s_type, :s_literal
        when :predicate
          value_type, literal_extra = :p_type, :p_literal
        when :object
          value_type, literal_extra = :o_type, :o_literal
        when :graph_name
          value_type, literal_extra = :c_type, :c_literal
        end

        h = case entity
        when RDF::URI
          { place_in_statement => entity.to_s, value_type => :uri }
        when RDF::Node
          { place_in_statement => entity.id.to_s, value_type => :node }
        when RDF::Literal
          if entity.has_language?
            { place_in_statement => entity.value, value_type => :literal_lang, literal_extra => entity.language.to_s }
          elsif entity.has_datatype?
            { place_in_statement => entity.value, value_type => :literal_type, literal_extra => entity.datatype.to_s }
          else
            { place_in_statement => entity.value, value_type => :literal }
          end
        else
          {}
        end

        h.select { |_, value| !value.nil? }
      end

      def self.p_to_mongo(pattern, place_in_statement)
        case place_in_statement
        when :subject
          value_type, literal_extra = :s_type, :s_literal
        when :predicate
          value_type, literal_extra = :p_type, :p_literal
        when :object
          value_type, literal_extra = :o_type, :o_literal
        when :graph_name
          value_type, literal_extra = :c_type, :c_literal
        end

        h = case pattern
        when RDF::Query::Variable, Symbol
          # Returns anything other than the default context
          { place_in_statement => nil, value_type => {"$ne" => :default} }
        when false
          # Used for the default context
          { place_in_statement => false, value_type => :default}
        else
          return self.entity_to_mongo(pattern, place_in_statement)
        end

        h.select { |_, value| !value.nil? }
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

      ##
      # Create BSON for a statement representation. Note that if the statement has no graph name,
      # a value of `false` will be used to indicate the default context
      #
      # @param [RDF::Statement] statement
      # @return [Hash] Generated BSON representation of statement.
      def self.statement_from_mongo(statement)
        RDF::Statement.new(
          subject:    RDF::Mongo::Conversion.from_mongo(statement['subject'],   statement['s_type'], statement['s_literal']),
          predicate:  RDF::Mongo::Conversion.from_mongo(statement['predicate'], statement['p_type'], statement['p_literal']),
          object:     RDF::Mongo::Conversion.from_mongo(statement['object'],    statement['o_type'], statement['o_literal']),
          graph_name: RDF::Mongo::Conversion.from_mongo(statement['graph_name'],   statement['c_type'], statement['c_literal']))
      end

      ##
      # Creates a BSON representation of the statement.
      # @return [Hash]
      def self.statement_to_mongo(statement)
        h = statement.to_hash.inject({}) do |hash, (place_in_statement, entity)|
          hash.merge(RDF::Mongo::Conversion.entity_to_mongo(entity, place_in_statement))
        end
        h[:c_type] ||= :default # Indicate statement is in the default graph
        h
      end

      def self.pattern_to_mongo(pattern)
        h = pattern.to_hash.inject({}) do |hash, (place_in_statement, entity)|
          hash.merge(RDF::Mongo::Conversion.p_to_mongo(entity, place_in_statement))
        end
        h.merge!(graph_name: nil, c_type: :default) if pattern.graph_name == false # TODO: refactor this into #pattern_to_mongo
        h
      end
    end
  end
end