# frozen_string_literal: true
require "test_helper"

module RapidFlow
  class BatchErrorHandlingTest < Minitest::Test
    def test_error_handling_captures_exceptions
      batch = Batch.build do
        stage ->(data) {
          raise "Error in stage 1" if data == "bad"
          data
        }
        stage ->(data) { data.upcase }
      end

      batch.push("good")
      batch.push("bad")

      results = batch.results

      assert_equal 2, results.length

      # Good result should complete both stages
      assert_equal "GOOD", results[0][0]
      assert_nil results[0][1]

      # Bad result should have error from stage 1 and not be processed by stage 2
      assert_equal "bad", results[1][0] # Original data preserved
      assert_instance_of RuntimeError, results[1][1]
      assert_equal "Error in stage 1", results[1][1].message
    end

    def test_error_in_middle_stage
      batch = Batch.build do
        stage ->(data) { data.upcase }
        stage ->(data) {
          raise "Error in stage 2" if data == "BAD"
          data
        }
        stage ->(data) { data + "!" }
      end

      batch.push("good")
      batch.push("bad")
      batch.push("also_good")

      results = batch.results

      assert_equal 3, results.length
      assert_equal ["GOOD!", nil], results[0]
      assert_equal ["BAD", results[1][1]], [results[1][0], results[1][1]]
      assert_equal "Error in stage 2", results[1][1].message
      assert_equal ["ALSO_GOOD!", nil], results[2]
    end

    def test_error_in_last_stage
      batch = Batch.build do
        stage ->(data) { data.upcase }
        stage ->(data) {
          raise "Error in final stage" if data == "BAD"
          data
        }
      end

      batch.push("good")
      batch.push("bad")

      results = batch.results

      assert_equal 2, results.length
      assert_equal ["GOOD", nil], results[0]
      assert_equal ["BAD", results[1][1]], [results[1][0], results[1][1]]
      assert_equal "Error in final stage", results[1][1].message
    end

    def test_multiple_errors_in_sequence
      batch = Batch.build do
        stage ->(data) {
          raise "Error at #{data}" if data.start_with?("bad")
          data
        }
      end

      batch.push("good1")
      batch.push("bad1")
      batch.push("bad2")
      batch.push("good2")

      results = batch.results

      assert_equal 4, results.length
      assert_equal ["good1", nil], results[0]
      assert_instance_of RuntimeError, results[1][1]
      assert_instance_of RuntimeError, results[2][1]
      assert_equal ["good2", nil], results[3]
    end

    def test_exception_types_preserved
      batch = Batch.build do
        stage ->(data) {
          case data
          when "argument_error"
            raise ArgumentError, "Bad argument"
          when "runtime_error"
            raise "Runtime problem"
          when "custom_error"
            raise StandardError, "Custom error"
          else
            data
          end
        }
      end

      batch.push("good")
      batch.push("argument_error")
      batch.push("runtime_error")
      batch.push("custom_error")

      results = batch.results

      assert_equal 4, results.length
      assert_equal ["good", nil], results[0]
      assert_instance_of ArgumentError, results[1][1]
      assert_instance_of RuntimeError, results[2][1]
      assert_instance_of StandardError, results[3][1]
    end

    def test_all_items_fail
      batch = Batch.build do
        stage ->(data) { raise "Always fails" }
      end

      5.times { |i| batch.push(i) }

      results = batch.results

      assert_equal 5, results.length
      results.each do |result, error|
        assert_instance_of RuntimeError, error
        assert_equal "Always fails", error.message
      end
    end
  end
end
