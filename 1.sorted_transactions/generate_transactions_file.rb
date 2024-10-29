require 'securerandom'
require 'time'

def generate_transactions_file_size(file_path, size_in_gb, start_time = Time.now, max_amount = 1000.0)
  size_in_bytes = size_in_gb * 1024**3
  current_size = 0

  File.open(file_path, 'w') do |file|
    while current_size < size_in_bytes
      timestamp = (start_time + rand(0..100_000)).utc.iso8601
      transaction_id = "txn#{SecureRandom.hex(4)}"
      user_id = "user#{rand(1000..9999)}"
      amount = format('%.2f', rand(1.0..max_amount))
      line = "#{timestamp},#{transaction_id},#{user_id},#{amount}\n"
      
      file.write(line)
      current_size += line.bytesize
    end
  end
end

generate_transactions_file_size('input_transactions.txt', 1) # 1 ГБ
