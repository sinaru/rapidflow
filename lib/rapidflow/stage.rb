# frozen_string_literal: true

module RapidFlow
  # Represents a processing stage in the pipeline
  class Stage
    def initialize(stage_index:, lambda_fn:, workers:, is_final:, pipeline:)
      validate_worker!(workers)

      @stage_index = stage_index
      @lambda_fn = lambda_fn
      @workers = workers
      @is_final = is_final
      @pipeline = pipeline
    end

    def start
      @workers.times do
        thread = Thread.new { work_loop }
        @pipeline.register_thread(thread)
      end
    end

    private

    def work_loop
      loop do
        work_item = @pipeline.dequeue(@stage_index)
        break if work_item == :shutdown

        process_item(work_item)
      end
    rescue ThreadError
      # Queue closed
    end

    def process_item(work_item)
      # Skip processing if item already has an error
      if work_item.has_error?
        forward_item(work_item)
        return
      end

      processed_item = execute_lambda(work_item)
      forward_item(processed_item)
    end

    def execute_lambda(work_item)
      result = @lambda_fn.call(work_item.data)
      work_item.data = result
      work_item
    rescue => e
      # Don't change 'work_item.data' to preserve previous data on error
      work_item.error = e # only add error
      work_item
    end

    def forward_item(work_item)
      @pipeline.enqueue(@stage_index + 1, work_item)
      @pipeline.decrement_active_workers if @is_final
    end

    def validate_worker!(workers)
      return if workers.kind_of?(Integer) && workers.positive?

      raise RapidFlow::ConfigError, "Worker count should be a positive number for stage"
    end
  end
end
