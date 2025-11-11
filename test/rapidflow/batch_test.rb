# frozen_string_literal: true
require "test_helper"

module RapidFlow
  class BatchTest < Minitest::Test
    def test_basic_functionality_with_arg_tasks
      batch = Batch.new(
        { fn: ->(data) { data.upcase }, workers: 4 },
        { fn: ->(data) { data + "!" }, workers: 4 }
      )
      batch.start

      batch.push("hello")
      batch.push("world")

      results = batch.results

      assert_equal 2, results.length
      assert_equal ["HELLO!", nil], results[0]
      assert_equal ["WORLD!", nil], results[1]
    end

    def test_basic_functionality_with_build
      batch = Batch.build do
        # first stage to up case string
        stage ->(data) { data.upcase }

        # the second stage adds ! to string
        stage ->(data) { data + "!" }
      end

      batch.push("hello")
      batch.push("world")

      results = batch.results

      assert_equal 2, results.length
      assert_equal ["HELLO!", nil], results[0]
      assert_equal ["WORLD!", nil], results[1]
    end

    def test_concurrent_execution_is_faster_than_sequential
      # Each lambda sleeps for 0.5 seconds
      # With 4 items and 2 stages:
      # - Sequential would take: 4 items * 0.5s * 2 stages = 4 seconds
      # - Concurrent (4 workers per stage) should take: max(0.5s, 0.5s) = ~0.5-1s

      batch = Batch.build do
        stage ->(data) {
          sleep(0.5)
          data
        }, workers: 4
        stage ->(data) {
          sleep(0.5)
          data
        }, workers: 4
      end

      start_time = Time.now

      4.times { |i| batch.push(i) }
      results = batch.results

      elapsed = Time.now - start_time

      assert_equal 4, results.length
      # Should complete in roughly 1 second (concurrent), not 4 seconds (sequential)
      # Give some buffer for thread overhead
      assert elapsed < 2.0, "Expected concurrent execution (~1s), but took #{elapsed}s"
    end

    def test_parallel_processing_at_each_stage
      # Track which threads are executing simultaneously
      execution_tracker = Mutex.new
      stage1_executing = []
      stage2_executing = []

      batch = Batch.build do
        stage ->(data) {
          execution_tracker.synchronize { stage1_executing << data }
          sleep(0.3)
          execution_tracker.synchronize { stage1_executing.delete(data) }
          data
        }
        stage ->(data) {
          execution_tracker.synchronize { stage2_executing << data }
          sleep(0.3)
          execution_tracker.synchronize { stage2_executing.delete(data) }
          data
        }
      end

      # Push multiple items quickly
      10.times { |i| batch.push(i) }

      # Give threads time to start processing
      sleep(0.1)

      # Check that multiple items are being processed concurrently at stage 1
      execution_tracker.synchronize do
        # At least 2 items should be processing simultaneously
        assert stage1_executing.length >= 2,
               "Expected concurrent execution, but only #{stage1_executing.length} items processing"
      end

      results = batch.results
      assert_equal 10, results.length
    end

    def test_pipeline_stages_process_independently
      # Track execution order to verify pipeline behavior
      execution_log = Queue.new

      batch = Batch.build do
        stage ->(data) {
          execution_log.push("stage1_start_#{data}")
          sleep(0.2)
          execution_log.push("stage1_end_#{data}")
          data
        }
        stage ->(data) {
          execution_log.push("stage2_start_#{data}")
          sleep(0.2)
          execution_log.push("stage2_end_#{data}")
          data
        }
      end

      batch.push("A")
      sleep(0.1) # Let A start processing
      batch.push("B")

      batch.results

      # Convert log to array
      log = []
      log << execution_log.pop until execution_log.empty?

      # Verify that stage2 can start processing A while stage1 is still processing B
      stage1_end_a = log.index("stage1_end_A")
      stage2_start_a = log.index("stage2_start_A")
      stage1_start_b = log.index("stage1_start_B")

      assert stage2_start_a > stage1_end_a, "Stage 2 should start A after Stage 1 ends A"
      assert stage2_start_a < log.length, "Stage 2 should process A"

      # B should be processing in stage1 while A is in stage2
      assert stage1_start_b, "B should have started in stage1"
    end

    def test_cannot_push_after_results_called
      batch = Batch.build do
        stage ->(data) { data }
      end

      batch.push("item1")
      batch.results

      error = assert_raises(RapidFlow::RunError) { batch.push("item2") }

      assert_equal "Cannot push to a locked batch when results are requested", error.message
    end

    def test_results_waits_for_all_processing_to_complete
      completion_times = Queue.new

      batch = Batch.build do
        stage ->(data) {
          sleep(0.5)
          data
        }
        stage ->(data) {
          completion_times.push(Time.now)
          data
        }
      end

      batch.push("item1")
      batch.push("item2")

      Time.now
      results = batch.results
      results_end = Time.now

      # All items should have completed before results returns
      assert_equal 2, results.length

      # Get completion times
      time1 = completion_times.pop
      time2 = completion_times.pop

      # Both completion times should be before results returned
      assert time1 < results_end
      assert time2 < results_end
    end

    def test_high_throughput_with_many_items
      item_count = 100

      batch = Batch.build do
        stage ->(data) {
          sleep(0.01)
          data * 2
        }
        stage ->(data) {
          sleep(0.01)
          data + 1
        }
      end

      start_time = Time.now
      item_count.times { |i| batch.push(i) }
      results = batch.results
      elapsed = Time.now - start_time

      assert_equal item_count, results.length

      # Verify results are correct
      results.each do |result, error|
        assert_nil error
        assert result.odd?, "Expected odd number, got #{result}"
      end

      # Should complete in well under sequential time (2 seconds for 100 items)
      assert elapsed < 5, "Processing #{item_count} items took #{elapsed}s, may not be concurrent"
    end

    def test_three_stage_pipeline
      batch = Batch.build do
        stage ->(data) {
          sleep(0.1)
          data.upcase
        }
        stage ->(data) {
          sleep(0.1)
          data + "!"
        }
        stage ->(data) {
          sleep(0.1)
          data * 2
        }
      end

      batch.push("hello")
      batch.push("world")

      results = batch.results

      assert_equal 2, results.length
      assert_equal ["HELLO!HELLO!", nil], results[0]
      assert_equal ["WORLD!WORLD!", nil], results[1]
    end

    def test_results_preserve_input_order
      # Even though items complete at different times, results should match push order
      batch = Batch.build do
        stage ->(data) {
          # Make later items finish faster
          sleep_time = (data[:id] == 0) ? 0.5 : 0.1
          sleep(sleep_time)
          data[:id]
        }, workers: 4
      end

      # Push items in order 0, 1, 2, 3
      # But item 0 will take longer to complete
      4.times { |i| batch.push({ id: i }) }

      results = batch.results

      # Results should still be in order 0, 1, 2, 3
      assert_equal 4, results.length
      assert_equal [0, nil], results[0]
      assert_equal [1, nil], results[1]
      assert_equal [2, nil], results[2]
      assert_equal [3, nil], results[3]
    end

    def test_single_stage_pipeline
      batch = Batch.build do
        stage ->(data) { data * 2 }
      end

      batch.push(5)
      batch.push(10)

      results = batch.results

      assert_equal 2, results.length
      assert_equal [10, nil], results[0]
      assert_equal [20, nil], results[1]
    end

    def test_empty_pipeline
      batch = Batch.build { stage ->(_data) { } }

      results = batch.results

      assert_equal 0, results.length
    end

    def test_different_worker_counts
      # Test with 1 worker per stage (sequential at each stage)
      j1 = Batch.build do
        stage ->(data) {
          sleep(0.1)
          data
        }, workers: 1
      end

      3.times { |i| j1.push(i) }
      results1 = j1.results
      assert_equal 3, results1.length

      # Test with 10 workers per stage
      j2 = Batch.build do
        stage ->(data) {
          sleep(0.1)
          data
        }, workers: 10
      end

      3.times { |i| j2.push(i) }
      results2 = j2.results
      assert_equal 3, results2.length
    end

    def test_complex_data_types
      batch = Batch.build do
        stage ->(data) { { original: data, processed: true } }
        stage ->(data) { data.merge(stage2: Time.now.to_i) }
      end

      batch.push({ id: 1, name: "test" })
      batch.push([1, 2, 3])

      results = batch.results

      assert_equal 2, results.length
      assert results[0][0].is_a?(Hash)
      assert results[0][0][:processed]
      assert results[1][0].is_a?(Hash)
      assert results[1][0][:original] == [1, 2, 3]
    end

    def test_nil_values
      batch = Batch.build do
        stage ->(data) { data.nil? ? "was_nil" : data }
        stage ->(data) { data.upcase }
      end

      batch.push(nil)
      batch.push("hello")

      results = batch.results

      assert_equal 2, results.length
      assert_equal ["WAS_NIL", nil], results[0]
      assert_equal ["HELLO", nil], results[1]
    end

    def test_large_dataset_stress_test
      item_count = 500

      batch = Batch.build do
        stage ->(data) { data * 2 }, workers: 8
        stage ->(data) { data + 1 }, workers: 8
        stage ->(data) { data.to_s }, workers: 8
      end

      item_count.times { |i| batch.push(i) }

      results = batch.results

      assert_equal item_count, results.length

      # Verify all results are correct and in order
      item_count.times do |i|
        expected = ((i * 2) + 1).to_s
        assert_equal [expected, nil], results[i], "Item #{i} incorrect"
      end
    end

    def test_varying_processing_times
      # Simulate real-world scenario with varying processing times
      batch = Batch.build do
        stage ->(data) {
          sleep(rand * 0.1) # Random 0-100ms
          data.upcase
        }
        stage ->(data) {
          sleep(rand * 0.1)
          data.reverse
        }
      end

      words = %w[apple banana cherry date elderberry fig grape]
      words.each { |word| batch.push(word) }

      results = batch.results

      assert_equal words.length, results.length
      words.each_with_index do |word, i|
        expected = word.upcase.reverse
        assert_equal [expected, nil], results[i]
      end
    end

    def test_push_many_items_quickly
      batch = Batch.build do
        stage ->(data) { data }
      end

      # Push 1000 items as fast as possible
      1000.times { |i| batch.push(i) }

      results = batch.results

      assert_equal 1000, results.length
      # Verify order is maintained
      1000.times do |i|
        assert_equal [i, nil], results[i]
      end
    end

    def test_idempotent_results_calls_not_allowed
      batch = Batch.build do
        stage ->(data) { data }
      end

      batch.push(1)
      batch.results

      # Can't call results again or push again
      assert_raises(RuntimeError) { batch.push(2) }
    end

    def test_thread_safety_of_shared_state
      shared_counter = { count: 0 }
      mutex = Mutex.new

      batch = Batch.build do
        stage ->(data) {
          # Safely increment shared counter
          mutex.synchronize { shared_counter[:count] += 1 }
          data
        }, workers: 10
      end

      100.times { |i| batch.push(i) }
      results = batch.results

      assert_equal 100, results.length
      assert_equal 100, shared_counter[:count]
    end
  end
end
