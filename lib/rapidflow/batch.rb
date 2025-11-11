# frozen_string_literal: true

module RapidFlow
  class Batch
    class ConfigError < RuntimeError; end
    class RunError < RuntimeError; end

    # DSL entrypoint
    def self.build(&block)
      builder = Builder.new
      builder.instance_eval(&block) if block
      belt = new(*builder.stages)
      belt.start
      belt
    end

    # Initialize with a list of stage configs: { fn: -> (input) { }, workers: Integer }, ...
    def initialize(*stage_configs)
      @lambdas = stage_configs.map { |s| s[:fn] }
      @workers_per_stage = stage_configs.map { |s| s[:workers] }
      @pipeline = Pipeline.new(@lambdas.length, @workers_per_stage)
      @stages = build_stages

      @item_counter = Counter.new

      # to track if no more items can be added
      @locked = false
      @locked_mutex = Mutex.new

      # to track if belt is running
      @running = false
      @running_mutex = Mutex.new
    end

    def start
      raise ConfigError, "Unable to start the belt without any stages" if @stages.empty?

      @stages.each(&:start)
      mark_run!
    end

    def push(data)
      ensure_not_finalized!

      work_item = WorkItem.new(@item_counter.next_index, data, nil)
      @pipeline.enqueue(0, work_item)
    end

    def results
      ensure_running!
      finalize!
      @pipeline.wait_for_completion
      @pipeline.shutdown
      mark_stop!
      collect_and_sort_results
    end

    private

    # DSL builder
    class Builder
      attr_reader :stages

      def initialize
        @stages = []
      end

      def stage(lambda_fn, workers: 4)
        @stages << { fn: lambda_fn, workers: workers }
      end
    end

    def build_stages
      stages = []
      @lambdas.each_with_index do |lambda_fn, stage_index|
        stages << Stage.new(
          stage_index: stage_index,
          lambda_fn: lambda_fn,
          workers: @workers_per_stage[stage_index],
          is_final: stage_index == @lambdas.length - 1,
          pipeline: @pipeline
        )
      end

      stages
    end

    def mark_stop!
      @running_mutex.synchronize { @running = false }
    end

    def mark_run!
      @running_mutex.synchronize { @running = true }
    end

    def finalize!
      @locked_mutex.synchronize { @locked = true }
    end

    def ensure_not_finalized!
      @locked_mutex.synchronize do
        raise RunError, "Cannot push to a locked belt when results are requested" if @locked
      end
    end

    def ensure_running!
      @running_mutex.synchronize do
        raise RunError, "Batch has not started" unless @running
      end
    end

    def collect_and_sort_results
      results = []
      results << @pipeline.dequeue_result until @pipeline.results_empty?

      results
        .sort_by { |item| item.index }
        .map { |item| [item.data, item.error] }
    end
  end
end
