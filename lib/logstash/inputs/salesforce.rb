# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "socket" # for Socket.gethostname

class LogStash::Inputs::Salesforce < LogStash::Inputs::Base
# Setting the config_name here is required. This is how you
# configure this input from your Logstash config.
#
# input {
#   salesforce { ... }
# }
config_name "salesforce"
# If undefined, Logstash will complain, even if codec is unused.
default :codec, "plain"

# Set this to true to connect to a sandbox sfdc instance
# logging in through test.salesforce.com
config :test, :validate => :boolean, :default => false
# Consumer Key for authentication. You must set up a new SFDC
# connected app with oath to use this output. More information
# can be found here:
# https://help.salesforce.com/apex/HTViewHelpDoc?id=connected_app_create.htm
config :client_id, :validate => :string, :required => true
# Consumer Secret from your oauth enabled connected app
config :client_secret, :validate => :string, :required => true
# A valid salesforce user name, usually your email address.
# Used for authentication and will be the user all objects
# are created or modified by
config :username, :validate => :string, :required => true
# The password used to login to sfdc
config :password, :validate => :string, :required => true
# The security token for this account. For more information about
# generting a security token, see:
# https://help.salesforce.com/apex/HTViewHelpDoc?id=user_security_token.htm
config :security_token, :validate => :string, :required => true
# The name of the salesforce object you are creating or updating
config :sfdc_object_name, :validate => :string, :required => true
# These are the field names to return in the Salesforce query
# If this is empty, all fields are returned.
config :sfdc_fields, :validate => :array, :default => []

public
  def register
    require 'restforce'
    @host = Socket.gethostname
    if @test
      @client = Restforce.new :host           => 'test.salesforce.com',
                              :username       => @username,
                              :password       => @password,
                              :security_token => @security_token,
                              :client_id      => @client_id,
                              :client_secret  => @client_secret
    else
      @client = Restforce.new :username       => @username,
                              :password       => @password,
                              :security_token => @security_token,
                              :client_id      => @client_id,
                              :client_secret  => @client_secret
    end
    if @sfdc_fields.empty?
      obj_desc = @client.describe(@sfdc_object_name)
      @sfdc_fields = get_fields(obj_desc)
    end
  end # def register

public
  def run(queue)
    results = @client.query(get_query())
    if results.first
      results.each do |result|
        event = LogStash::Event.new()
        decorate(event)
        event['host'] = @host
        @sfdc_fields.each do |field|
          event[field.downcase] = result.__send__(field)
        end
        queue << event
      end
    end
  end # def run

public
  def teardown
  end # def teardown

private
  def get_query()
    query = "SELECT "+@sfdc_fields.join(',')+" FROM "+@sfdc_object_name+" WHERE IsDeleted = false ORDER BY LastModifiedDate DESC"
    @logger.debug("SFDC Query: "+query)
    return query
  end

  def get_fields(obj_desc)
    fields = []
    obj_desc.fields.each do |f|
      fields.push(f.name)
    end
    return fields
  end

end # class LogStash::Inputs::Salesforce
