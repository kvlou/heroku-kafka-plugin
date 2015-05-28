$:.unshift File.join(File.dirname(__FILE__), 'vendor')

require 'rubygems'
require 'bundler'
ENV['BUNDLE_GEMFILE'] = File.join(File.dirname(__FILE__), 'Gemfile')
Bundler.setup

require 'zookeeper'
require 'poseidon'
require 'uri'
require 'timeout'

module Heroku::Helpers::Kafka
  def valid?(url)
    data = JSON.parse(url)
    !data.nil?
  rescue
    false
  end

  def attachment(args)
    config_vars = Heroku::Auth.api.get_config_vars(app).body
    name = args.first
    url = config_vars["#{args.first}_URL"]
    error "No such attachment #{name}" unless url
    error "Invalid URL on attachment #{name}" unless valid? url
    [name, url]
  end

end

class Heroku::Command::Produce < Heroku::Command::Base
  include Heroku::Helpers::Kafka

  # produce:one [KAFKA] [TOPIC] [DATA]
  #
  # list available topics
  #
  def one
    if args.length != 3
      error "Kafka name, topic and data to produce required"
    end
    _, topic, data = args
    name, url = attachment(args)
    parsed = JSON.parse(url, symbolize_names: true)
    url = URI.parse(parsed[:kafka])
    producer = Poseidon::Producer.new(["#{url.host}:#{url.port}"], "one-producer")
    display("Publishing data to topic #{topic}..")
    msg = Poseidon::MessageToSend.new(topic, data)
    producer.send_messages([msg])
    display("Done.")
  end

end

class Heroku::Command::Consume < Heroku::Command::Base
  include Heroku::Helpers::Kafka
  # topics:list [KAFKA]
  #
  # list available topics
  #
  def one
    if args.length != 2
      error "Kafka name, topic and data to consume required"
    end
    _, topic = args
    name, url = attachment(args)
    parsed = JSON.parse(url, symbolize_names: true)
    url = URI.parse(parsed[:kafka])
    consumer = Poseidon::PartitionConsumer.new("one-consumer", url.host, url.port, topic, 0, :earliest_offset)
    display("Consuming data from topic #{topic}..")
    msg = consumer.fetch
    display("--> #{msg.last.value}")
  end

end

class Heroku::Command::Topics < Heroku::Command::Base
  include Heroku::Helpers::Kafka
  # topics:list [KAFKA]
  #
  # list available topics
  #
  def list
    if args.length != 1
      error "Kafka name required"
    end
    name, url = attachment(args)
    get_topics(url, name)
    display
  end

  # topics:create [KAFKA] [TOPIC]
  #
  # create topic
  #
  def create
    if args.length != 2
      error "Kafka name and topic required"
    end
    name, url = attachment(args)
    create_topic(url, args.last)
    display("Creating topic #{args.last}..")
    display
    get_topics(url, name)
  end

  # topics:create [KAFKA] [TOPIC]
  #
  # create topic
  #
  def delete
    if args.length != 2
      error "Kafka name and topic required"
    end
    name, url = attachment(args)
    delete_topic(url, args.last)
    display("Deleting topic #{args.last}..")
    display
    get_topics(url, name)
  end

  private

  def create_topic(url, topic)
    data = JSON.parse(url, :symbolize_names => true)
    zk = URI.parse(data[:zk])
    z = Zookeeper.new("#{zk.host}:#{zk.port}")
    # register the topic first
    z.create(path: "/config/topics/#{topic}", data: topic_config.to_json)
    z.create(path: "/brokers/topics/#{topic}", data: broker_topic_registration.to_json)
  end

  def delete_topic(url, topic)
    data = JSON.parse(url, :symbolize_names => true)
    zk = URI.parse(data[:zk])
    z = Zookeeper.new("#{zk.host}:#{zk.port}")
    z.delete(path: "/config/topics/#{topic}")
    # z.create(path: "/admin/delete_topics/#{topic}")
  end


  def topic_config
    { version: 1, config: {}}
  end

  def broker_topic_registration
    { version: 1, partitions: {'0' => [1]}}
  end

  def get_topics(url, name)
    data = JSON.parse(url, :symbolize_names => true)
    zk = URI.parse(data[:zk])
    z = Zookeeper.new("#{zk.host}:#{zk.port}")
    r = z.get_children(path: '/config/topics')
    styled_header("Topic for Kafka cluster #{name}")
    if r[:children].length == 0
      display("No topics found.")
    else
      topic_data = {}
      r[:children].each do |topic|
        r = z.get(path: "/brokers/topics/#{topic}")
        config = JSON.parse(r[:data], symbolize_names: true)
        topic_data[topic] = "partitions -> #{config[:partitions].keys.length}, version -> #{config[:version]}"
      end
      styled_hash(topic_data)
    end
  end
end


class Heroku::Command::Kafka < Heroku::Command::Base
  include Heroku::Helpers::Kafka

  # kafka:wait [KAFKA]
  #
  # monitor kafka creation, exit when complete
  #
  def wait
    if args.length != 1
      error "Kafka name required"
    end
    name, urls = attachment(args)
    wait_for(urls, name, 3)
  end

  private

  def get_wait_status(attachment)
    data = JSON.parse(attachment, :symbolize_names => true)
    zk = data[:zookeeper]
    if zk_ready? zk
      {waiting?: false, message: 'ready!' }
    else
      {waiting?: true}
    end
  end

  def zk_ready?(addresses)
    r = Timeout::timeout(10) {
      res = addresses.map do |addr|
        z = Zookeeper.new(addr)
        r = z.stat(path: '/')
        r[:stat].exists?
      end.all? {|r| r == true}
    }
  rescue
    false
  end

  def ticking(interval)
    ticks = 0
    loop do
      yield(ticks)
      ticks +=1
      sleep interval
    end
  end

  def wait_for(url, name, interval)
    ticking(interval) do |ticks|
      status = get_wait_status(url)
      error status[:message] if status[:error?]
      break if !status[:waiting?] && ticks.zero?
      redisplay("Waiting for kafka %s... %s%s" % [
                  name,
                  status[:waiting?] ? "#{spinner(ticks)} " : "",
                  status[:message]],
                  !status[:waiting?]) # only display a newline on the last tick
      break unless status[:waiting?]
    end
  end
end
