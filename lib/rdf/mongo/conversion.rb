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
          value_type, position, literal_extra = :s_type, :subject, :s_literal
        when :predicate
          value_type, position, literal_extra = :p_type, :predicate, :p_literal
        when :object
          value_type, position, literal_extra = :o_type, :object, :o_literal
        when :graph_name
          value_type, position, literal_extra = :c_type, :context, :c_literal
        end

        case entity
        when RDF::URI
          pos, type = entity.to_s, :uri
        when RDF::Node
          pos, type = entity.id.to_s, :node
        when RDF::Literal
          if entity.has_language?
            pos, type, ll = entity.value, :literal_lang, entity.language.to_s
          elsif entity.has_datatype?
            pos, type, ll = entity.value, :literal_type, entity.datatype.to_s
          else
            pos, type, ll = entity.value, :literal, nil
          end
        end
=begin
        when nil # FIXME: shouldn't return anything
          pos, type = nil, nil
        else # FIXME: we should never get here
          pos, type = entity.to_s, :uri
        end
        pos = nil if pos == ''
        # =====
=end

        h = { position => pos, value_type => type, literal_extra => ll }
        h.delete_if {|kk,_| h[kk].nil?}
      end

      def self.p_to_mongo(pattern, place_in_statement)
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

        case pattern
        when RDF::Query::Variable, Symbol
          # Returns anything other than the default context
          pos, type = nil, {"$ne" => :default}
        when false
          # Used for the default context
          pos, type = false, :default
        when nil # FIXME: shouldn't return anything
        else
          return self.entity_to_mongo(pattern, place_in_statement)
        end

        h = { position => pos, value_type => type }
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
      # Creates a BSON representation of the statement.
      # @return [Hash]
      def self.statement_to_mongo(statement)
        statement.to_hash.inject({}) do |hash, (place_in_statement, entity)|
          hash.merge(RDF::Mongo::Conversion.entity_to_mongo(entity, place_in_statement))
        end
      end

      def self.pattern_to_mongo(pattern)
        pattern.to_hash.inject({}) do |hash, (place_in_statement, entity)|
          hash.merge(RDF::Mongo::Conversion.p_to_mongo(entity, place_in_statement))
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
          graph_name: RDF::Mongo::Conversion.from_mongo(statement['context'],   statement['c_type'], statement['c_literal']))
      end
    end
  end
end