# frozen_string_literal: true

require "test_helper"

module RapidFlow
  class PipelineTest < Minitest::Test
    def test_empty_pipeline
      pipeline = Pipeline.new(0, 1)

      pipeline.wait_for_completion
      pipeline.shutdown

      assert pipeline.results_empty?
    end

    def test_pipeline_with_single_stage
      pipeline = Pipeline.new(1, 1)

      pipeline.enqueue(0, "test_item")

      # Simulate stage processing
      item = pipeline.dequeue(0)
      pipeline.enqueue(1, item.upcase)
      pipeline.decrement_active_workers

      result = pipeline.dequeue_result

      assert_equal "TEST_ITEM", result

      pipeline.shutdown
    end

    def test_pipeline_queues_created_correctly
      pipeline = Pipeline.new(3, 2)

      # Pipeline with 3 stages should have 4 queues (one per stage + results queue)
      (0..3).each do |i|
        pipeline.enqueue(i, "item_#{i}")

        result = pipeline.dequeue(i)
        assert_equal "item_#{i}", result
      end

      pipeline.shutdown
    end

    def test_active_workers_tracking
      pipeline = Pipeline.new(1, 1)

      pipeline.enqueue(0, "item1")
      pipeline.enqueue(0, "item2")

      # Simulate processing
      pipeline.dequeue(0)
      pipeline.decrement_active_workers

      pipeline.dequeue(0)
      pipeline.decrement_active_workers

      pipeline.wait_for_completion

      pipeline.shutdown

      assert pipeline.results_empty?
    end
  end
end
