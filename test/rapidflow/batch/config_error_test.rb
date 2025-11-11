# frozen_string_literal: true
require "test_helper"

module RapidFlow
  class BatchConfigErrorTest < Minitest::Test
    def test_no_stages_with_build
      error = assert_raises(RapidFlow::ConfigError) do
        Batch.build do
          # no stages
        end
      end

      assert_equal "Unable to start the batch without any stages", error.message
    end

    def test_no_stages_batch_start
      error = assert_raises(RapidFlow::ConfigError) do
        batch = Batch.new
        batch.start
      end

      assert_equal "Unable to start the batch without any stages", error.message
    end

    def test_invalid_worker_count
      [
        -3,
        0,
        1.5,
        'foo',
        :bar
      ].each do |invalid_worker_count|
        error = assert_raises(RapidFlow::ConfigError, "Expected to raise exception for '#{invalid_worker_count}'") do
          Batch.new({ fn: ->(data) { data.upcase }, workers: invalid_worker_count })
        end

        assert_equal "Worker count should be a positive number for stage", error.message
      end
    end
  end
end


