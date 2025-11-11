# frozen_string_literal: true

module RapidFlow
  # Manages the queues and worker coordination
  class Pipeline
    # workers_per_stage can be an Integer (uniform) or an Array per stage
    def initialize(num_stages, workers_per_stage)
      @num_stages = num_stages
      @workers_per_stage = workers_per_stage
      @queues = Array.new(num_stages + 1) { Queue.new }
      @threads = []
      @active_workers = 0
      @mutex = Mutex.new
      @completion_cv = ConditionVariable.new
    end

    def enqueue(stage_index, work_item)
      increment_active_workers if stage_index == 0
      @queues[stage_index].push(work_item)
    end

    def dequeue(stage_index)
      @queues[stage_index].pop
    end

    def dequeue_result
      @queues.last.pop
    end

    def results_empty?
      @queues.last.empty?
    end

    def register_thread(thread)
      @threads << thread
    end

    def increment_active_workers
      @mutex.synchronize { @active_workers += 1 }
    end

    def decrement_active_workers
      @mutex.synchronize do
        @active_workers -= 1
        @completion_cv.signal if @active_workers == 0
      end
    end

    def wait_for_completion
      @mutex.synchronize do
        @completion_cv.wait(@mutex) while @active_workers > 0
      end
    end

    def shutdown
      # Send shutdown signals to all worker threads
      (0...@num_stages).each do |stage_index|
        worker_count =
          if @workers_per_stage.is_a?(Array)
            @workers_per_stage[stage_index] || 0
          else
            @workers_per_stage
          end
        worker_count.times { @queues[stage_index].push(:shutdown) }
      end
      @threads.each(&:join)
    end
  end
end
