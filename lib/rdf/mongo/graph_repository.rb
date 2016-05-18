require 'rdf/mongo'

class Hash
  # Returns a hash that includes everything but the given keys.
  #   hash = { a: true, b: false, c: nil}
  #   hash.except(:c) # => { a: true, b: false}
  #   hash # => { a: true, b: false, c: nil}
  #
  # This is useful for limiting a set of parameters to everything but a few known toggles:
  #   @person.update(params[:person].except(:admin))
  def except(*keys)
    dup.except!(*keys)
  end

  # Replaces the hash without the given keys.
  #   hash = { a: true, b: false, c: nil}
  #   hash.except!(:c) # => { a: true, b: false}
  #   hash # => { a: true, b: false }
  def except!(*keys)
    keys.each { |key| delete(key) }
    self
  end

  def slice(*keys)
    Hash[[keys, self.values_at(*keys)].transpose]
  end

  def compact
    delete_if { |k, v| v.nil? }
  end
end

module RDF
  module Mongo

    class Conversion
      ##
      # Split a BSON representation of the statement into
      # context and triple components
      #
      # @return [Hash] BSON representation of the statement
      def self.split_mongo(bson)
        # context = { ct: :default }.merge(bson).slice(:c, :ct, :cl).compact
        {context: bson.slice(:c, :ct, :cl),
          triple: bson.except!(:c, :ct, :cl)}
      end
    end

    class GraphRepository < ::RDF::Mongo::Repository

      def initialize(options = {}, &block)
        options[:collection] ||= 'graphs'
        options[:indexes] = [
          {key: {'c' => 'hashed'}},
          {key: {'ct' => 'hashed'}},
          {key: {'cl' => 'hashed'}},
          {key: {'statements.s' => 1}},
          {key: {'statements.p' => 1}},
          {key: {'statements.o' => 1}},
          {key: {'statements.s' => 1, 'statements.p' => 1}},
          {key: {'statements.s' => 1, 'statements.p' => 1, 'statements.o' => 1}},
        ]
        super(options, &block)
      end

      def insert_statement(statement)
        raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?

        split = RDF::Mongo::Conversion.split_mongo(statement.to_mongo)
        split[:context][:ct] ||= :default # Indicate statement is in the default graph

        doc = @collection.find_one_and_update(split[:context], {
                                              "$setOnInsert" => split[:context],
                                              "$addToSet" => { statements: split[:triple]}
                                              }, :upsert => true)
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        split = RDF::Mongo::Conversion.split_mongo(statement.to_mongo)

        case statement.graph_name
        when nil
          @collection.find_one_and_update(split[:context].merge(ct: :default), { "$pullAll" => { statements: [split[:triple]]} })
        else
          @collection.find_one_and_update(split[:context], { "$pullAll" => { statements: [split[:triple]]} })
        end
      end

      def count
        agg = @collection.aggregate([{ :$project => { :statements => { :$size => "$statements" } } }])
        agg.map { |g| g[:statements] }.reduce(:+) || 0
      end

      def empty?; count == 0; end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        split = RDF::Mongo::Conversion.split_mongo(statement.to_mongo)
        split[:context][:ct] ||= :default # Indicate statement is in the default graph
        query = split[:context].merge({statements: { :$elemMatch => split[:triple] }})

        @collection.find(query).count > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        if block_given?
          # TODO: investigate parallel scan
          @collection.find().each do |document|
            document[:statements].each do |statement|
              context = {'c'  => document[:c], 'ct' => document[:ct], 'cl' => document[:cl]}
              block.call(RDF::Statement.from_mongo(context.merge(statement)))
            end
          end
        end
        enum_statement
      end
      alias_method :each, :each_statement

      protected

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?

        split = RDF::Mongo::Conversion.split_mongo(pattern.to_mongo)
        split[:context].merge!(c: nil, ct: :default) if pattern.graph_name == false

        query = split[:context].compact.merge({statements: { :$elemMatch => split[:triple] }})

        @collection.find(query).each do |document|
          context = {'c'  => document[:c], 'ct' => document[:ct], 'cl' => document[:cl]}

          document[:statements].each do |statement|
            s = RDF::Statement.from_mongo(context.merge(statement))
            block.call(s) if pattern === s # If the pattern matches the statement (undocumented in RDF::Statement)
          end
        end
      end

    end
  end
end
