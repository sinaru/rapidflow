# frozen_string_literal: true

module RapidFlow
  # DSL builder
  class BatchBuilder
    attr_reader :stages

    def initialize
      @stages = []
    end

    def stage(lambda_fn, workers: 4)
      @stages << { fn: lambda_fn, workers: workers }
    end
  end
end
