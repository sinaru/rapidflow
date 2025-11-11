# frozen_string_literal: true

module RapidFlow
  # Tracks item indices for ordering
  class Counter
    def initialize
      @next_index = 0
      @mutex = Mutex.new
    end

    def next_index
      @mutex.synchronize do
        index = @next_index
        @next_index += 1
        index
      end
    end
  end
end
