# frozen_string_literal: true
require "test_helper"

module RapidFlow
  class BatchConfigErrorTest < Minitest::Test
    def test_no_stages_with_build
      error = assert_raises(Batch::ConfigError) do
        Batch.build do
          # no stages
        end
      end

      assert_equal "Unable to start the batch without any stages", error.message
    end

    def test_no_stages_batch_start
      error = assert_raises(Batch::ConfigError) do
        batch = Batch.new
        batch.start
      end

      assert_equal "Unable to start the batch without any stages", error.message
    end
  end
end


