require "test_helper"

module RapidFlow
  class WorkItemTest < Minitest::Test
    def test_default_initialization
      item = WorkItem.new
      assert_nil item.index
      assert_nil item.data
      assert_nil item.error
      refute item.has_error?
    end

    def test_attributes
      item = WorkItem.new(5, "payload")
      assert_equal 5, item.index
      assert_equal "payload", item.data

      # Update index and data
      item.index = 42
      item.data = {ok: true}
      assert_equal 42, item.index
      assert_equal({ok: true}, item.data)
    end

    def test_attribute_accessors_and_has_error
      item = WorkItem.new(0, nil, nil)
      refute item.has_error?

      # Set an error and verify has_error? is true
      item.error = StandardError.new("boom")
      assert item.has_error?

      # Edge case: empty string is still considered an error because it's non-nil
      item.error = ""
      assert item.has_error?

      # Clearing the error should make has_error? false again
      item.error = nil
      refute item.has_error?
    end
  end
end
