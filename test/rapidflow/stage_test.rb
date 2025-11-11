# frozen_string_literal: true

require "test_helper"

module Rapidflow
  class StageTest < Minitest::Test
    def test_stage_processes_single_item
      pipeline = Pipeline.new(1, 1)
      work_item = WorkItem.new(data: 5)

      stage = Stage.new(
        stage_index: 0,
        lambda_fn: ->(data) { data * 2 },
        workers: 1,
        is_final: true,
        pipeline: pipeline
      )

      stage.start
      pipeline.enqueue(0, work_item)

      result = pipeline.dequeue_result
      pipeline.shutdown

      assert_equal 10, result.data
      refute result.has_error?
    end

    def test_stage_handles_errors
      pipeline = Pipeline.new(1, 1)
      work_item = WorkItem.new(data: "test")

      stage = Stage.new(
        stage_index: 0,
        lambda_fn: ->(_data) { raise StandardError, "Processing error" },
        workers: 1,
        is_final: true,
        pipeline: pipeline
      )

      stage.start
      pipeline.enqueue(0, work_item)

      result = pipeline.dequeue_result
      pipeline.shutdown

      assert_equal "test", result.data
      assert result.has_error?
      assert_instance_of StandardError, result.error
      assert_equal "Processing error", result.error.message
    end

    def test_stage_with_multiple_workers
      pipeline = Pipeline.new(1, 3)
      items = 10.times.map { |i| WorkItem.new(data: i) }

      stage = Stage.new(
        stage_index: 0,
        lambda_fn: ->(data) { data * 2 },
        workers: 3,
        is_final: true,
        pipeline: pipeline
      )

      stage.start
      items.each { |item| pipeline.enqueue(0, item) }

      results = []
      10.times { results << pipeline.dequeue_result }
      pipeline.shutdown

      assert_equal 10, results.length
      results.each do |result|
        refute result.has_error?
        assert_includes (0..9).map { |i| i * 2 }, result.data
      end
    end

    def test_stage_forwards_to_next_stage
      pipeline = Pipeline.new(2, 1)
      work_item = WorkItem.new(data: "hello")

      stage1 = Stage.new(
        stage_index: 0,
        lambda_fn: ->(data) { data.upcase },
        workers: 1,
        is_final: false,
        pipeline: pipeline
      )

      stage2 = Stage.new(
        stage_index: 1,
        lambda_fn: ->(data) { data + "!" },
        workers: 1,
        is_final: true,
        pipeline: pipeline
      )

      stage1.start
      stage2.start
      pipeline.enqueue(0, work_item)

      result = pipeline.dequeue_result
      pipeline.shutdown

      assert_equal "HELLO!", result.data
      refute result.has_error?
    end
  end
end
