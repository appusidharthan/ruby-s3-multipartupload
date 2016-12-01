#!/usr/bin/ruby
require 'digest/md5'
require 'rubygems'
require 'yaml'
require 'aws-sdk'

class File
  def each_part(part_size=PART_SIZE)
    yield read(part_size) until eof?
  end
end

PART_SIZE=1024*1024*500
localfile='testfile.txt'
filebasename = File.basename(localfile)
bucket='lsstore'
key='test/appu/50m-mpu'
current_part = 1

input_opts = {
	bucket: bucket,
	key:    key,
}

s3 = Aws::S3::Client.new(
  region: 'us-east-1',
  credentials: Aws::Credentials.new('yourawskey','yourawssecret'),
)

File.delete("checksum.txt") if File.exist?("checksum.txt")
File.open(localfile, 'rb') do |file|
	mpu_create_response = s3.create_multipart_upload(input_opts)
	total_parts = file.size.to_f / PART_SIZE 
	file.each_part do |part|
		digest = Digest::MD5.hexdigest(part)
		File.open('checksum.txt', 'a') { |file| file.write(digest+"\n") }
		part_response = s3.upload_part({
			body:        part,
			bucket:      bucket,
			key:         key,
			part_number: current_part,
			upload_id:   mpu_create_response.upload_id,
		})  
		percent_complete = (current_part.to_f / total_parts.to_f) * 100
		percent_complete = 100 if percent_complete > 100 
		percent_complete = sprintf('%.2f', percent_complete.to_f)
		puts "percent complete: #{percent_complete}"
		current_part = current_part + 1
	end
	input_opts = input_opts.merge({
		:upload_id   => mpu_create_response.upload_id,
	})
	parts_resp = s3.list_parts(input_opts)
	input_opts = input_opts.merge(
		:multipart_upload => {
			:parts => parts_resp.parts.map do |part|
				{ 	:part_number => part.part_number,
					:etag        => part.etag }
	              end 
			}   
		)   
	mpu_complete_response = s3.complete_multipart_upload(input_opts)
end
resp = s3.get_object({
	key: key,
	bucket: bucket,
})

digest = `xxd -r -p checksum.txt | md5sum`.gsub(/\s+-.*/, "").chomp!
puts digest
GetOutput = YAML.load(resp.to_yaml)
RemoteEtag= GetOutput["etag"].gsub(/\"+|-.*/, "")
puts RemoteEtag