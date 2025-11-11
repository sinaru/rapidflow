#!/usr/bin/env ruby

# Example usage:
# ruby benchmark_images.rb ~/Pictures/sample.jpg 100 8 # Process 100 images with 8 workers

require "benchmark"
require "fileutils"
require "mini_magick"
require_relative "../../lib/rapidflow"

class ImageProcessor
  def self.load_image(path)
    MiniMagick::Image.open(path)
  end

  def self.resize_image(image)
    image.resize("800x600")
    image
  end

  def self.apply_filter(image)
    image.colorspace("Gray")
    image
  end

  def self.save_image(image, output_dir)
    output_path = File.join(output_dir, "processed_#{rand(500)}_#{rand(500)}.#{image.type.downcase}")
    image.write(output_path)
    image
  end
end

# Create test images directory by cloning a sample image
def setup_test_images(sample_image_path, count = 50)
  unless File.exist?(sample_image_path)
    raise "Sample image not found: #{sample_image_path}"
  end

  test_dir = "tmp/benchmark_images"
  FileUtils.mkdir_p(test_dir)

  puts "Cloning sample image: #{sample_image_path}"
  extension = File.extname(sample_image_path)

  images = []
  count.times do |i|
    filename = "image_#{i.to_s.rjust(3, "0")}#{extension}"
    filepath = File.join(test_dir, filename)

    # Clone the sample image
    FileUtils.cp(sample_image_path, filepath)
    images << filepath
  end

  images
end

# Cleanup
def cleanup_test_images
  FileUtils.rm_rf("tmp/benchmark_images")
  FileUtils.rm_rf("tmp/output_sync")
  FileUtils.rm_rf("tmp/output_rapidflow")
end

# Solution 1: Synchronous processing (no threads)
def process_images_synchronously(image_paths, output_dir)
  FileUtils.mkdir_p(output_dir)

  results = []

  image_paths.each do |path|
    # Stage 1: Load
    image = ImageProcessor.load_image(path)

    # Stage 2: Resize
    image = ImageProcessor.resize_image(image)

    # Stage 3: Apply filter
    image = ImageProcessor.apply_filter(image)

    # Stage 4: Save
    result = ImageProcessor.save_image(image, output_dir)

    results << [result, nil]
  rescue => e
    results << [path, e]
  end

  results
end

# Solution 2: RapidFlow concurrent processing
def process_images_with_rapidflow(image_paths, output_dir, workers: 4)
  FileUtils.mkdir_p(output_dir)

  belt = RapidFlow::Batch.build do
    # Stage 1: Load image
    stage ->(path) { ImageProcessor.load_image(path) }, workers: workers

    # Stage 2: Resize
    stage ->(image) { ImageProcessor.resize_image(image) }, workers: workers

    # Stage 3: Apply filter
    stage ->(image) { ImageProcessor.apply_filter(image) }, workers: workers

    # Stage 4: Save
    stage ->(image) { ImageProcessor.save_image(image, output_dir) }, workers: workers
  end

  image_paths.each { |path| belt.push(path) }
  belt.results
end

# Run benchmark
def run_benchmark(sample_image_path, image_count: 50, workers: 4)
  puts "=" * 80
  puts "RapidFlow Image Processing Benchmark"
  puts "=" * 80
  puts
  puts "Configuration:"
  puts "  Sample image: #{sample_image_path}"
  puts "  Images to process: #{image_count}"
  puts "  Workers per stage: #{workers}"
  puts "  Stages: Load → Resize → Filter → Compress → Save"
  puts "  Simulated processing time per stage: 0.05-0.1 seconds"
  puts

  # Setup
  puts "Setting up test images..."
  image_paths = setup_test_images(sample_image_path, image_count)
  puts "Created #{image_paths.size} test images (cloned from sample)"
  puts

  # Benchmark synchronous
  puts "-" * 80
  puts "1. SYNCHRONOUS PROCESSING (No threads)"
  puts "-" * 80

  sync_time = nil
  sync_results = nil

  Benchmark.bm(30) do |x|
    sync_time = x.report("Synchronous:") do
      sync_results = process_images_synchronously(image_paths, "tmp/output_sync")
    end
  end

  sync_success = sync_results.count { |_, err| err.nil? }
  sync_failed = sync_results.count { |_, err| !err.nil? }
  missing_count = image_count - sync_results.count

  puts
  puts "Results: #{sync_success} successful, #{sync_failed} failed"
  if missing_count > 0
    puts "🚨 ERROR: #{missing_count} missing images. Something is wrong with processing pipeline. 🐛"
    exit 1
  end
  puts

  # Benchmark RapidFlow
  puts "-" * 80
  puts "2. RAPIDFLOW CONCURRENT PROCESSING"
  puts "-" * 80

  rapidflow_time = nil
  rapidflow_results = nil

  Benchmark.bm(30) do |x|
    rapidflow_time = x.report("RapidFlow (#{workers} workers):") do
      rapidflow_results = process_images_with_rapidflow(image_paths, "tmp/output_rapidflow", workers: workers)
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

  # Cleanup
  puts "Cleaning up test files..."
  cleanup_test_images
  puts "Done!"
  puts
end

# Main execution
if __FILE__ == $0
  # Parse command line arguments
  if ARGV.empty?
    puts "Error: Sample image path is required"
    puts
    puts "Usage: ruby benchmark_images.rb <sample_image_path> [image_count] [workers]"
    puts
    puts "Examples:"
    puts "  ruby benchmark_images.rb photo.jpg                    # 50 images, 4 workers"
    puts "  ruby benchmark_images.rb photo.jpg 100 8              # 100 images, 8 workers"
    puts "  ruby benchmark_images.rb images/sample.png 20 2       # 20 images, 2 workers"
    puts "  ruby benchmark_images.rb ~/Pictures/test.jpg 200 16   # 200 images, 16 workers"
    puts
    exit 1
  end

  sample_image_path = ARGV[0]
  image_count = (ARGV[1] || 50).to_i
  workers = (ARGV[2] || 4).to_i

  # Validate sample image exists
  unless File.exist?(sample_image_path)
    puts "Error: Sample image not found: #{sample_image_path}"
    puts
    puts "Please provide a valid path to an image file."
    exit 1
  end

  # Validate it's a file
  unless File.file?(sample_image_path)
    puts "Error: Path is not a file: #{sample_image_path}"
    exit 1
  end

  run_benchmark(sample_image_path, image_count: image_count, workers: workers)

  puts "=" * 80
  puts "Want to try different configurations?"
  puts
  puts "Usage: ruby benchmark_images.rb <sample_image_path> [image_count] [workers]"
  puts
  puts "Examples:"
  puts "  ruby benchmark_images.rb #{sample_image_path} 100 8    # Process 100 images with 8 workers"
  puts "  ruby benchmark_images.rb #{sample_image_path} 20 2     # Process 20 images with 2 workers"
  puts "  ruby benchmark_images.rb #{sample_image_path} 200 16   # Process 200 images with 16 workers"
  puts "=" * 80
end
