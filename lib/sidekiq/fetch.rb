require 'sidekiq'
require 'celluloid'

module Sidekiq
  ##
  # The Fetcher blocks on Redis, waiting for a message to process
  # from the queues.  It gets the message and hands it to the Manager
  # to assign to a ready Processor.
  class Fetcher
    include Celluloid
    include Sidekiq::Util

    TIMEOUT = 1
       
    def initialize(mgr, queues, strict, ignored_queues=[])
      @mgr = mgr
      @strictly_ordered_queues = strict
      @queues = queues.map { |q| "queue:#{q}" }
      @unique_queues = @queues.uniq
      @ignored_queues = ignored_queues
    end

    # Fetching is straightforward: the Manager makes a fetch
    # request for each idle processor when Sidekiq starts and
    # then issues a new fetch request every time a Processor
    # finishes a message.
    #
    # Because we have to shut down cleanly, we can't block
    # forever and we can't loop forever.  Instead we reschedule
    # a new fetch if the current fetch turned up nothing.
    def fetch
      watchdog('Fetcher#fetch died') do
        return if Sidekiq::Fetcher.done?

        begin
          queue, msg = Sidekiq.redis { |conn| conn.blpop(*queues_cmd) }

          if msg
            @mgr.async.assign(msg, queue.gsub(/.*queue:/, ''))
          else
            after(0) { fetch }
          end
        rescue => ex
          logger.error("Error fetching message: #{ex}")
          logger.error(ex.backtrace.first)
          sleep(TIMEOUT)
          after(0) { fetch }
        end
      end
    end

    # Ugh.  Say hello to a bloody hack.
    # Can't find a clean way to get the fetcher to just stop processing
    # its mailbox when shutdown starts.
    def self.done!
      @done = true
    end

    def self.done?
      @done
    end

    private

    # Creating the Redis#blpop command takes into account any
    # configured queue weights. By default Redis#blpop returns
    # data from the first queue that has pending elements. We
    # recreate the queue command each time we invoke Redis#blpop
    # to honor weights and avoid queue starvation.
    def queues_cmd
      queues=[]
      if Sidekiq.options[:round_robin]
        queues=Sidekiq.redis { |conn|
          conn.smembers('queues')
        }
        queues=(queues-@ignored_queues).map { |q| "queue:#{q}" }
        if queues.size >= 1
        @queues+=(queues-@queues)
        @queues-=(@queues-queues)
        queue=@queues.pop
        @queues.insert(0,queue)
        queues=@queues.dup
        else
          queues=["nil"]
        end
      elsif Sidekiq.options[:dynamic_queues]
        queues=Sidekiq.redis { |conn|
          conn.smembers('queues')
        }.map { |q| "queue:#{q}" }
        queues.concat(@unique_queues - queues)
      else
        return @unique_queues.dup << TIMEOUT if @strictly_ordered_queues
        queues = @queues.sample(@unique_queues.size).uniq
      end
      queues << TIMEOUT
    end
  end
end
