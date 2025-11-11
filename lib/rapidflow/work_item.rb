# frozen_string_literal: true

module RapidFlow
  # Represents a work item flowing through the pipeline
  WorkItem = Struct.new('WorkItem', :index, :data, :error) do
    def has_error?
      !error.nil?
    end
  end
end
