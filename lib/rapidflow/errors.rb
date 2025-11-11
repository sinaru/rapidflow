# frozen_string_literal: true

module RapidFlow
  # Error class to use when the client setup code is invalid.
  class ConfigError < RuntimeError; end
  class RunError < RuntimeError; end
end
