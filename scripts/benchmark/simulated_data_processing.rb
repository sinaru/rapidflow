# !/usr/bin/env ruby
require "benchmark"
require "fileutils"
require_relative "../../lib/rapidflow"

# Simulated data processing operations
# In real usage, replace with actual HTTP clients, HTML parsers, and database operations

class DataProcessor
  SLEEP_AMT_FETCH_HTML = 0.05
  SLEEP_AMT_PARSE_DATA = 0.1
  SLEEP_AMT_FETCH_OTHER_DATA = 0.08
  SLEEP_AMT_MERGE_DATA = 0
  SLEEP_AMT_SAVE_TO_DB = 0.05

  SLEEP_TOTAL_AMT = [
    SLEEP_AMT_FETCH_HTML,
    SLEEP_AMT_PARSE_DATA,
    SLEEP_AMT_FETCH_OTHER_DATA,
    SLEEP_AMT_MERGE_DATA,
    SLEEP_AMT_SAVE_TO_DB
  ].sum

  def self.fetch_html(url)
    # Simulate fetching HTML from URL (I/O operation)
    sleep(0.05)
    {
      url: url,
      html: "<html><body><h1>Sample Data #{rand(1000)}</h1><p>Content from #{url}</p></body></html>",
      status: 200,
      headers: {"content-type" => "text/html"}
    }
  end

  def self.parse_data(html_response)
    # Simulate parsing HTML and extracting data
    sleep(SLEEP_AMT_PARSE_DATA)
    {
      title: html_response[:html].match(/<h1>(.*?)<\/h1>/)[1],
      content: html_response[:html].match(/<p>(.*?)<\/p>/)[1],
      url: html_response[:url],
      parsed_at: Time.now
    }
  end

  def self.fetch_other_data(data)
    # Simulate another HTTP request to get additional data
    sleep(SLEEP_AMT_FETCH_OTHER_DATA)
    additional_data = {
      external_id: rand(10000),
      category: ["tech", "news", "blog", "article"].sample,
      metadata: {
        word_count: data[:content].split.size,
        fetched_at: Time.now
      }
    }
    [data, additional_data]
  end

  def self.merge_data(data, additional_data)
    data.merge(additional_data)
  end

  def self.save_to_db(data)
    # Simulate saving to database (I/O operation)
    sleep(SLEEP_AMT_SAVE_TO_DB)
    data.merge(
      id: rand(100000),
      saved_at: Time.now,
      saved: true
    )
  end
end

# Create test URLs
def setup_test_urls(count = 50)
  urls = []
  count.times do |i|
    urls << "https://example.com/page_#{i.to_s.rjust(3, "0")}"
  end
  urls
end

# Solution 1: Synchronous processing (no threads)
def process_data_synchronously(urls)
  results = []

  urls.each do |url|
    # Stage 1: Fetch HTML
    html_response = DataProcessor.fetch_html(url)

    # Stage 2: Parse data
    parsed_data = DataProcessor.parse_data(html_response)

    # Stage 3: Fetch other data
    data, additional_data = DataProcessor.fetch_other_data(parsed_data)

    # Stage 4: Merge data
    enriched_data = DataProcessor.merge_data(data, additional_data)

    # Stage 5: Save to database
    result = DataProcessor.save_to_db(enriched_data)

    results << [result, nil]
  rescue => e
    results << [url, e]
  end

  results
end

# Solution 2: RapidFlow concurrent processing
def process_data_with_rapidflow(urls, workers: 4)
  belt = RapidFlow::Batch.build do
    stage ->(url) { DataProcessor.fetch_html(url) }, workers: workers # Station 1: Fetch HTML
    stage ->(html) { DataProcessor.parse_data(html) }, workers: workers # Station 2: Parse data
    stage ->(data) { DataProcessor.fetch_other_data(data) }, workers: workers # Station 3: Fetch other data
    stage ->((data, additional_data)) { DataProcessor.merge_data(data, additional_data) }, workers: workers # Station 4: Merge data
    stage ->(data) { DataProcessor.save_to_db(data) }, workers: workers # Station 5: Save to a database
  end

  urls.each { |url| belt.push(url) }
  belt.results
end

# Run benchmark
def run_benchmark(url_count: 50, workers: 4)
  puts "=" * 80
  puts "RapidFlow Data Processing Benchmark"
  puts "=" * 80
  puts
  puts "Configuration:"
  puts "  URLs to process: #{url_count}"
  puts "  Workers per stage: #{workers}"
  puts "  Stages: Fetch HTML → Parse data → Fetch other data → Merge data → Save to database"
  puts "  Simulated processing time per stage: 0.05-0.1 seconds (merge has no delay)"
  puts

  # Setup
  puts "Setting up test URLs..."
  urls = setup_test_urls(url_count)
  puts "Created #{urls.size} test URLs"
  puts

  # Benchmark synchronous
  puts "-" * 80
  puts "1. SYNCHRONOUS PROCESSING (No threads)"
  puts "-" * 80

  sync_time = nil
  sync_results = nil

  Benchmark.bm(30) do |x|
    sync_time = x.report("Synchronous:") do
      sync_results = process_data_synchronously(urls)
    end
  end

  sync_success = sync_results.count { |_, err| err.nil? }
  sync_failed = sync_results.count { |_, err| !err.nil? }

  puts
  puts "Results: #{sync_success} successful, #{sync_failed} failed"
  puts

  # Benchmark RapidFlow
  puts "-" * 80
  puts "2. RAPIDFLOW CONCURRENT PROCESSING"
  puts "-" * 80

  rapidflow_time = nil
  rapidflow_results = nil

  Benchmark.bm(30) do |x|
    rapidflow_time = x.report("RapidFlow (#{workers} workers):") do
      rapidflow_results = process_data_with_rapidflow(urls, workers: workers)
    end
  end

  rapidflow_success = rapidflow_results.count { |_, err| err.nil? }
  rapidflow_failed = rapidflow_results.count { |_, err| !err.nil? }

  puts
  puts "Results: #{rapidflow_success} successful, #{rapidflow_failed} failed"
  puts

  # Calculate speedup
  sync_real_time = sync_time.real
  rapidflow_real_time = rapidflow_time.real
  speedup = sync_real_time / rapidflow_real_time
  time_saved = sync_real_time - rapidflow_real_time
  percent_faster = ((speedup - 1) * 100).round(1)

  # Summary
  puts "=" * 80
  puts "SUMMARY"
  puts "=" * 80
  puts
  puts "Synchronous time:     #{sync_real_time.round(2)}s"
  puts "RapidFlow time:       #{rapidflow_real_time.round(2)}s"
  puts
  puts "Speedup:              #{speedup.round(2)}x faster"
  puts "Time saved:           #{time_saved.round(2)}s"
  puts "Performance gain:     #{percent_faster}%"
  puts

  # Theoretical vs actual
  total_processing_time = DataProcessor::SLEEP_TOTAL_AMT # Sum of all stage times (merge has 0 delay)
  theoretical_sync = url_count * total_processing_time
  theoretical_concurrent = (url_count.to_f / workers) * total_processing_time
  theoretical_speedup = theoretical_sync / theoretical_concurrent

  puts "Theoretical analysis:"
  puts "  Expected sync time:      ~#{theoretical_sync.round(2)}s"
  puts "  Expected concurrent:     ~#{theoretical_concurrent.round(2)}s"
  puts "  Expected speedup:        ~#{theoretical_speedup.round(2)}x"
  puts "  Actual speedup:          #{speedup.round(2)}x"
  puts "  Efficiency:              #{((speedup / theoretical_speedup) * 100).round(1)}%"
  puts
end

# Main execution
if __FILE__ == $0
  # Parse command line arguments
  url_count = (ARGV[0] || 50).to_i
  workers = (ARGV[1] || 4).to_i

  run_benchmark(url_count: url_count, workers: workers)

  puts "=" * 80
  puts "Want to try different configurations?"
  puts
  puts "Usage: ruby simulated_data_processing.rb [url_count] [workers]"
  puts
  puts "Examples:"
  puts "  ruby simulated_data_processing.rb 100 8    # Process 100 URLs with 8 workers"
  puts "  ruby simulated_data_processing.rb 20 2     # Process 20 URLs with 2 workers"
  puts "  ruby simulated_data_processing.rb 200 16   # Process 200 URLs with 16 workers"
  puts "=" * 80
end
