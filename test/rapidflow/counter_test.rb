require "test_helper"

module RapidFlow
  class CounterTest < Minitest::Test
    def test_sequential_indices
      counter = Counter.new
      assert_equal 0, counter.next_index
      assert_equal 1, counter.next_index
      assert_equal 2, counter.next_index
    end

    def test_thread_safety_and_uniqueness
      counter = Counter.new
      threads = []
      results = Queue.new

      thread_count = 20
      per_thread = 25

      thread_count.times do
        threads << Thread.new do
          per_thread.times do
            results << counter.next_index
          end
        end
      end

      threads.each(&:join)

      gathered = []
      gathered << results.pop until results.empty?

      total = thread_count * per_thread
      assert_equal total, gathered.length, "Expected #{total} indices generated"
      assert_equal total, gathered.uniq.length, "Indices must be unique"

      # Should be a contiguous range from 0..total-1 in some order
      assert_equal (0...total).to_a, gathered.sort
    end
  end
end
