$:.unshift "."
require 'spec_helper'

require 'rdf/spec/repository'
require 'rdf/mongo/graph_repository'

describe RDF::Mongo::GraphRepository do
  before :all do
    logger = RDF::Spec.logger
    logger.level = Logger::FATAL
    @load_durable = lambda {RDF::Mongo::GraphRepository.new uri: "mongodb://localhost:27017/rdf-mongo/specs", logger: logger}
    @repository = @load_durable.call
  end
  before :each do
    @repository.collection.drop
  end

  after :each do
    @repository.collection.drop
  end

  # @see lib/rdf/spec/repository.rb in RDF-spec
  it_behaves_like "an RDF::Repository" do
    let(:repository) {@repository}
  end

  context "problematic examples" do
    subject {@repository}
    {
      "Insert key too large to index" => %(
        <http://repository.librario.de/publications/0cbdc7f4-728d-4f85-ab09-01060c7b2922> <http://purl.org/ontology/bibo/abstract> "#{'a' * 1001}" .
      )
    }.each do |name, nt|
      it name do
        expect {subject << RDF::Graph.new << RDF::NTriples::Reader.new(nt)}.not_to raise_error
      end
    end
  end
end
