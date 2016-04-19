
require 'hanami/model/coercer'
require 'hanami/model/mapping/coercers'

class RethinkDBId < Hanami::Model::Coercer

  def self.load(value)
    loaded   = value["generated_keys"][0] rescue nil
    loaded ||= value                      rescue nil
    loaded
  end

  def self.dump(value)
    Hanami::Model::Mapping::Coercers::String.dump(value)
  end

end
