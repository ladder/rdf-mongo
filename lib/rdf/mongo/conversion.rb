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
      # @param [:subject, :predicate, :object, :graph_name] position
      #   Position within statement.
      # @return [Hash] BSON representation of the statement
      def self.entity_to_mongo(entity, position)
        value_type = "#{position.to_s.chr}_type".to_sym

        case entity
        when RDF::URI
          { position => entity.to_s, value_type => :uri }
        when RDF::Node
          { position => entity.id.to_s, value_type => :node }
        when RDF::Literal
          if entity.has_language?
            literal_extra = "#{position.to_s.chr}_literal".to_sym
            { position => entity.value, value_type => :literal_lang, literal_extra => entity.language.to_s }
          elsif entity.has_datatype?
            literal_extra = "#{position.to_s.chr}_literal".to_sym
            { position => entity.value, value_type => :literal_type, literal_extra => entity.datatype.to_s }
          else
            { position => entity.value, value_type => :literal }
          end
        else
          {}
        end
      end

      def self.p_to_mongo(pattern, position)
        value_type = "#{position.to_s.chr}_type".to_sym

        case pattern
        when RDF::Query::Variable, Symbol
          # Returns anything other than the default context
          { value_type => {"$ne" => :default} }
        when false
          # Used for the default context
          { position => false, value_type => :default}
        else
          return self.entity_to_mongo(pattern, position)
        end
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
      def self.statement_from_mongo(document)
        RDF::Statement.new(
          subject:    RDF::Mongo::Conversion.from_mongo(document['subject'],    document['s_type'], document['s_literal']),
          predicate:  RDF::Mongo::Conversion.from_mongo(document['predicate'],  document['p_type'], document['p_literal']),
          object:     RDF::Mongo::Conversion.from_mongo(document['object'],     document['o_type'], document['o_literal']),
          graph_name: RDF::Mongo::Conversion.from_mongo(document['graph_name'], document['g_type'], document['c_literal']))
      end

      ##
      # Creates a BSON representation of the statement.
      # @return [Hash]
      def self.statement_to_mongo(statement)
        h = statement.to_hash.inject({}) do |hash, (position, entity)|
          hash.merge(RDF::Mongo::Conversion.entity_to_mongo(entity, position))
        end
        h[:g_type] ||= :default # Indicate statement is in the default graph
        h
      end

      def self.pattern_to_mongo(pattern)
        h = pattern.to_hash.inject({}) do |hash, (position, entity)|
          hash.merge(RDF::Mongo::Conversion.p_to_mongo(entity, position))
        end
        h.merge!(graph_name: nil, g_type: :default) if pattern.graph_name == false
        h
      end
    end
  end
end