require 'mongoid'

module RDF
  module Mongoid
    class Graph
      include ::Mongoid::Document

      field :c
      field :ct, default: :default
      field :statements, type: Array

      index({c: 1})
      index({ct: 1})

      store_in collection: 'graphs'

      before_save { project_graph if changed? }

      def project_graph
        self.statements = RDF::Mongoid::Statement.where({c: self.c, ct: self.ct}).map { |s| s.attributes }
      end
    end
  end
end
