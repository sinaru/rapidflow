#!/usr/bin/env ruby

# Example usage:
# ruby benchmark_api_request_process_and_storing.rb 30 8  # Process 30 user IDs with 8 workers

require "benchmark"
require "fileutils"
require "net/http"
require "uri"
require "json"
require_relative "../../lib/rapidflow"

class ApiClient
  # Fetch user data from dummyjson.com API
  def self.fetch_user(user_id)
    uri = URI("https://dummyjson.com/users/#{user_id}")
    response = Net::HTTP.get_response(uri)

    if response.is_a?(Net::HTTPSuccess)
      JSON.parse(response.body)
    else
      raise "Failed to fetch user #{user_id}: #{response.code} #{response.message}"
    end
  end

  # Fetch product data from dummyjson.com API
  def self.fetch_product(product_id)
    uri = URI("https://dummyjson.com/products/#{product_id}")
    response = Net::HTTP.get_response(uri)

    if response.is_a?(Net::HTTPSuccess)
      JSON.parse(response.body)
    else
      raise "Failed to fetch product #{product_id}: #{response.code} #{response.message}"
    end
  end

  # Merge user and product data
  def self.merge_data(user_json, product_json)
    user_json["product"] = product_json
    user_json
  end

  # Save JSON data to file
  def self.save_to_file(user_id, data, output_dir)
    FileUtils.mkdir_p(output_dir)
    filename = File.join(output_dir, "data_#{user_id}.json")
    File.write(filename, JSON.pretty_generate(data))
    { user_id: user_id, file: filename, success: true }
  end
end

# Cleanup directories
def cleanup_output_dirs
  FileUtils.rm_rf("tmp/output_sync")
  FileUtils.rm_rf("tmp/output_rapidflow")
end

# Solution 1: Synchronous processing (no threads)
def process_data_synchronously(user_ids, output_dir)
  FileUtils.mkdir_p(output_dir)
  results = []

  user_ids.each do |user_id|
    # Stage 1: Fetch user data
    user_json = ApiClient.fetch_user(user_id)

    # Stage 2: Fetch product data (using same ID)
    product_json = ApiClient.fetch_product(user_id)

    # Stage 3: Merge data
    merged_data = ApiClient.merge_data(user_json, product_json)

    # Stage 4: Save to file
    result = ApiClient.save_to_file(user_id, merged_data, output_dir)

    results << [result, nil]
  rescue => e
    results << [user_id, e]
  end

  results
end

# Solution 2: Rapidflow concurrent processing
def process_data_with_rapidflow(user_ids, output_dir, workers: 8)
  FileUtils.mkdir_p(output_dir)

  belt = Rapidflow::Batch.build do
    # Stage 1: Fetch user data from API
    stage ->(user_id) {
      ApiClient.fetch_user(user_id)
    }, workers: workers

    # Stage 2: Fetch product data from API (using user_id as product_id)
    stage ->(user_json) {
      user_id = user_json["id"]
      product_json = ApiClient.fetch_product(user_id)
      [user_json, product_json]
    }, workers: workers

    # Stage 3: Merge user and product data
    stage ->((user_json, product_json)) {
      user_id = user_json["id"]
      merged_data = ApiClient.merge_data(user_json, product_json)
      [user_id, merged_data]
    }, workers: 2

    # Stage 4: Save to file
    stage ->((user_id, merged_data)) {
      ApiClient.save_to_file(user_id, merged_data, output_dir)
    }, workers: 4
  end

  user_ids.each { |user_id| belt.push(user_id) }
  belt.results
end

# Run benchmark
def run_benchmark(max_user_id: 30, workers: 8)
  puts "=" * 80
  puts "Rapidflow API Request, Process & Store Benchmark"
  puts "=" * 80
  puts
  puts "Configuration:"
  puts "  API: dummyjson.com"
  puts "  User IDs to process: 1 to #{max_user_id}"
  puts "  Workers per stage (Rapidflow): #{workers}"
  puts "  Stages: Fetch User → Fetch Product → Merge Data → Save to File"
  puts

  # Setup
  user_ids = (1..max_user_id).to_a
  puts "Processing #{user_ids.length} user IDs..."
  puts

  # Cleanup old output directories
  cleanup_output_dirs

  # Benchmark synchronous
  puts "-" * 80
  puts "1. SYNCHRONOUS PROCESSING (No threads)"
  puts "-" * 80

  sync_time = nil
  sync_results = nil

  Benchmark.bm(30) do |x|
    sync_time = x.report("Synchronous:") do
      sync_results = process_data_synchronously(user_ids, "tmp/output_sync")
    end
  end

  sync_success = sync_results.count { |_, err| err.nil? }
  sync_failed = sync_results.count { |_, err| !err.nil? }

  puts
  puts "Results: #{sync_success} successful, #{sync_failed} failed"

  if sync_failed > 0
    puts "\nFailed items:"
    sync_results.each_with_index do |(data, error), index|
      if error
        puts "  User ID #{user_ids[index]}: #{error.class} - #{error.message}"
      end
    end
  end
  puts

  # Benchmark Rapidflow
  puts "-" * 80
  puts "2. RAPIDFLOW CONCURRENT PROCESSING"
  puts "-" * 80

  rapidflow_time = nil
  rapidflow_results = nil

  Benchmark.bm(30) do |x|
    rapidflow_time = x.report("Rapidflow (#{workers} workers):") do
      rapidflow_results = process_data_with_rapidflow(user_ids, "tmp/output_rapidflow", workers: workers)
    end
  end

  rapidflow_success = rapidflow_results.count { |_, err| err.nil? }
  rapidflow_failed = rapidflow_results.count { |_, err| !err.nil? }

  puts
  puts "Results: #{rapidflow_success} successful, #{rapidflow_failed} failed"

  if rapidflow_failed > 0
    puts "\nFailed items:"
    rapidflow_results.each_with_index do |(data, error), index|
      if error
        puts "  User ID #{user_ids[index]}: #{error.class} - #{error.message}"
      end
    end
  end
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
  puts "Rapidflow time:       #{rapidflow_real_time.round(2)}s"
  puts
  puts "Speedup:              #{speedup.round(2)}x faster"
  puts "Time saved:           #{time_saved.round(2)}s"
  puts "Performance gain:     #{percent_faster}%"
  puts

  # File verification
  puts "-" * 80
  puts "FILE VERIFICATION"
  puts "-" * 80

  sync_files = Dir.glob("tmp/output_sync/data_*.json").length
  rapidflow_files = Dir.glob("tmp/output_rapidflow/data_*.json").length

  puts "Synchronous output:   #{sync_files} files created"
  puts "Rapidflow output:     #{rapidflow_files} files created"
  puts

  # Sample file content verification
  if rapidflow_files > 0
    sample_file = Dir.glob("tmp/output_rapidflow/data_*.json").first
    sample_data = JSON.parse(File.read(sample_file))

    puts "Sample output file: #{File.basename(sample_file)}"
    puts "  User ID: #{sample_data['id']}"
    puts "  User Name: #{sample_data['firstName']} #{sample_data['lastName']}"
    puts "  Has product data: #{sample_data.key?('product')}"
    if sample_data['product']
      puts "  Product ID: #{sample_data['product']['id']}"
      puts "  Product Title: #{sample_data['product']['title']}"
    end
    puts
  end

  # Performance analysis
  puts "-" * 80
  puts "PERFORMANCE ANALYSIS"
  puts "-" * 80
  puts

  avg_time_per_item_sync = sync_real_time / max_user_id
  avg_time_per_item_rapid = rapidflow_real_time / max_user_id

  puts "Average time per item:"
  puts "  Synchronous:  #{(avg_time_per_item_sync * 1000).round(2)}ms"
  puts "  Rapidflow:    #{(avg_time_per_item_rapid * 1000).round(2)}ms"
  puts

  throughput_sync = max_user_id / sync_real_time
  throughput_rapid = max_user_id / rapidflow_real_time

  puts "Throughput (items/second):"
  puts "  Synchronous:  #{throughput_sync.round(2)} items/sec"
  puts "  Rapidflow:    #{throughput_rapid.round(2)} items/sec"
  puts

  # Cleanup prompt
  puts "-" * 80
  puts "OUTPUT FILES"
  puts "-" * 80
  puts
  puts "Synchronous files: tmp/output_sync/"
  puts "Rapidflow files:   tmp/output_rapidflow/"
  puts
  puts "To clean up output directories, run:"
  puts "  rm -rf tmp/output_sync tmp/output_rapidflow"
  puts
end

# Main execution
if __FILE__ == $0
  max_user_id = (ARGV[0] || 30).to_i
  workers = (ARGV[1] || 8).to_i

  # Validate arguments
  if max_user_id < 1
    puts "Error: max_user_id must be at least 1"
    puts
    puts "Usage: ruby benchmark_api_request_process_and_storing.rb [max_user_id] [workers]"
    puts
    puts "Examples:"
    puts "  ruby benchmark_api_request_process_and_storing.rb              # 30 users, 8 workers"
    puts "  ruby benchmark_api_request_process_and_storing.rb 50 16        # 50 users, 16 workers"
    puts "  ruby benchmark_api_request_process_and_storing.rb 10 4         # 10 users, 4 workers"
    puts "  ruby benchmark_api_request_process_and_storing.rb 100 20       # 100 users, 20 workers"
    puts
    exit 1
  end

  if workers < 1
    puts "Error: workers must be at least 1"
    exit 1
  end

  # Warning for large datasets
  if max_user_id > 100
    puts "⚠️  Warning: Processing #{max_user_id} items will make many API requests."
    puts "This may take a while and could hit API rate limits."
    puts
    print "Continue? (y/n): "
    response = STDIN.gets.chomp.downcase
    unless response == "y" || response == "yes"
      puts "Cancelled."
      exit 0
    end
    puts
  end

  run_benchmark(max_user_id: max_user_id, workers: workers)

  puts "=" * 80
  puts "Want to try different configurations?"
  puts
  puts "Usage: ruby benchmark_api_request_process_and_storing.rb [max_user_id] [workers]"
  puts
  puts "Examples:"
  puts "  ruby benchmark_api_request_process_and_storing.rb 50 16    # Process 50 users with 16 workers"
  puts "  ruby benchmark_api_request_process_and_storing.rb 10 4     # Process 10 users with 4 workers"
  puts "  ruby benchmark_api_request_process_and_storing.rb 100 20   # Process 100 users with 20 workers"
  puts "=" * 80
end