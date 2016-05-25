# for debugging
# require 'pry'

require 'rdf/mongoid/statement'
require 'rdf/mongoid/graph'

module RDF
  module Mongoid
    class Repository < ::RDF::Repository

      def initialize(options = {}, &block)
        raise ArgumentError, "options[:uri] argument is missing" unless options[:uri]

        mongoid_opts = { clients: { default: { uri: options[:uri] } } }
        ::Mongoid.load_configuration(mongoid_opts)

        RDF::Mongoid::Graph.create_indexes
        RDF::Mongoid::Statement.create_indexes

        super(options, &block)
      end

      def supports?(feature)
        # TODO: transactions
        case feature.to_sym
          when :graph_name then true
          when :validity   then @options.fetch(:with_validity, true)
          else false
        end
      end

      def insert_statement(statement)
        raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
        st_mongo = statement.to_mongo
        st_mongo[:ct] ||= :default # Indicate statement is in the default graph

        RDF::Mongoid::Statement.collection.update_one(st_mongo, st_mongo, upsert: true)
      end

      def delete_statement(statement)
        st_mongo = statement.to_mongo
        st_mongo[:ct] = :default if statement.graph_name.nil?

        RDF::Mongoid::Statement.where(st_mongo).delete
      end

      def empty?; RDF::Mongoid::Statement.empty?; end

      def count; RDF::Mongoid::Statement.count; end

      def clear_statements; RDF::Mongoid::Statement.delete_all; end

      def has_statement?(statement)
        RDF::Mongoid::Statement.where(statement.to_mongo).count > 0
      end

      def each_statement(&block)
        if block_given?
          # TODO: investigate parallel scan with mongoid
          RDF::Mongoid::Statement.each do |document|
            block.call(RDF::Statement.from_mongo(document))
          end
        end
        enum_statement
      end
      alias_method :each, :each_statement

      def has_graph?(value)
        RDF::Mongoid::Statement.where(RDF::Mongo::Conversion.to_mongo(value, :graph_name)).count > 0
      end

      protected

      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?

        # A pattern graph_name of `false` is used to indicate the default graph
        pm = pattern.to_mongo
        pm.merge!(c: nil, ct: :default) if pattern.graph_name == false

        RDF::Mongoid::Statement.where(pm).each do |document|
          block.call(RDF::Statement.from_mongo(document))
        end
      end
    end
  end
end
