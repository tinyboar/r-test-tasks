require 'tempfile'
require 'fileutils'
require 'benchmark'
require 'etc'

DEFAULT_CHUNK_SIZE_MB = 50       # Размер чанка по умолчанию (в МБ)
DEFAULT_MAX_THREADS = Etc.nprocessors  # Количество потоков по умолчанию

# Пользовательские настройки (измените значения ниже, если необходимо)
CHUNK_SIZE_MB = DEFAULT_CHUNK_SIZE_MB
MAX_THREADS = DEFAULT_MAX_THREADS

# Проверка, были ли параметры изменены вручную
chunk_size_custom = (CHUNK_SIZE_MB != DEFAULT_CHUNK_SIZE_MB)
max_threads_custom = (MAX_THREADS != DEFAULT_MAX_THREADS)

# Вывод информации о параметрах
puts "Запуск программы с параметрами:"
if chunk_size_custom
  puts "Размер чанка задан вручную: #{CHUNK_SIZE_MB} МБ"
else
  puts "Размер чанка по умолчанию: #{CHUNK_SIZE_MB} МБ"
end

if max_threads_custom
  puts "Количество потоков задано вручную: #{MAX_THREADS}"
else
  puts "Количество потоков по умолчанию: #{MAX_THREADS}"
end

class Transaction
  attr_accessor :timestamp, :transaction_id, :user_id, :amount

  def initialize(line)
    @timestamp, @transaction_id, @user_id, amount_str = line.strip.split(',')
    @amount = amount_str.to_f
  end

  def to_s
    "#{@timestamp},#{@transaction_id},#{@user_id},#{'%.2f' % @amount}"
  end

  # Оператор сравнения для сортировки
  def <=>(other)
    @amount <=> other.amount
  end
end

# Ваш алгоритм сортировки merge_sort
def merge_sort(arr)
  return arr if arr.length <= 1

  mid = arr.length / 2
  left = merge_sort(arr[0...mid])
  right = merge_sort(arr[mid...arr.length])
  merge(left, right)
end

def merge(left, right)
  sorted = []
  until left.empty? || right.empty?
    if (left.first <=> right.first) >= 0
      sorted << left.shift
    else
      sorted << right.shift
    end
  end
  sorted + left + right
end

# Куча для слияния чанков
class MaxHeap
  def initialize
    @data = []
  end

  # Добавление элемента в кучу
  def push(element)
    @data << element
    sift_up(@data.size - 1)
  end

  # Извлечение максимального элемента из кучи
  def pop
    return nil if @data.empty?

    max = @data.first
    if @data.size == 1
      @data.pop
    else
      @data[0] = @data.pop
      sift_down(0)
    end
    max
  end

  def empty?
    @data.empty?
  end

  private

  # Восстановление свойства кучи вверх
  def sift_up(index)
    parent = (index - 1) / 2
    if index > 0 && (@data[index][0] <=> @data[parent][0]) > 0
      swap(index, parent)
      sift_up(parent)
    end
  end

  # Восстановление свойства кучи вниз
  def sift_down(index)
    child = index * 2 + 1
    return if child >= @data.size

    right_child = child + 1
    if right_child < @data.size && (@data[child][0] <=> @data[right_child][0]) < 0
      child = right_child
    end
    if (@data[index][0] <=> @data[child][0]) < 0
      swap(index, child)
      sift_down(child)
    end
  end

  # Обмен элементов в массиве кучи
  def swap(i, j)
    @data[i], @data[j] = @data[j], @data[i]
  end
end

def merge_sorted_files(temp_files, output_file_path)
  start_merge_time = Time.now
  heap = MaxHeap.new
  file_handlers = []

  # Открываем каждый временный файл и помещаем первую строку в кучу
  temp_files.each_with_index do |file_path, index|
    file = File.open(file_path, 'r')
    file_handlers << file
    line = file.gets
    if line
      transaction = Transaction.new(line)
      heap.push([transaction, index])
    end
  end

  # Открываем выходной файл для записи
  File.open(output_file_path, 'w') do |output_file|
    until heap.empty?
      # Извлекаем максимальный элемент из кучи
      max_transaction, file_index = heap.pop
      output_file.puts max_transaction.to_s

      # Считываем следующую строку из того же файла
      next_line = file_handlers[file_index].gets
      if next_line
        next_transaction = Transaction.new(next_line)
        heap.push([next_transaction, file_index])
      end
    end
  end

  file_handlers.each(&:close)
  puts "Время слияния чанков: #{(Time.now - start_merge_time).round(4)} секунд"
end

def unique_filename(directory, basename = 'chunk', extension = '.txt')
  timestamp = Time.now.to_f.to_s.gsub('.', '')
  random = rand(1000..9999)
  filename = "#{basename}_#{timestamp}_#{random}#{extension}"
  filepath = File.join(directory, filename)
  return filepath
end

def compute_avg_line_size(file_path, num_samples = 1000)
  total_size = 0.0
  line_count = 0
  File.open(file_path, 'r') do |file|
    num_samples.times do
      break if file.eof?
      line = file.gets
      total_size += line.bytesize
      line_count += 1
    end
  end
  avg_line_size = total_size / line_count
  puts "Средний размер строки: #{avg_line_size.round(2)} байт"
  avg_line_size
end

def external_sort(input_file_path, output_file_path, chunk_size_mb, max_threads)
  total_time = Benchmark.realtime do
    temp_dir = 'temp_chunks'
    FileUtils.mkdir_p(temp_dir) unless Dir.exist?(temp_dir)
    temp_files = []
    mutex = Mutex.new

    # Устанавливаем размер чанка
    chunk_size_in_bytes = chunk_size_mb * 1024 * 1024

    # Шаг 1: Последовательное чтение файла и обработка чанков в параллельных потоках
    chunk_creation_time = Benchmark.realtime do
      File.open(input_file_path, 'r') do |file|
        chunk_index = 0
        threads = []
        until file.eof?
          transactions = []
          bytes_read = 0

          # Чтение чанка из файла
          while bytes_read < chunk_size_in_bytes && !file.eof?
            line = file.gets
            transactions << Transaction.new(line)
            bytes_read += line.bytesize
          end

          # Обработка чанка в отдельном потоке
          thread = Thread.new(transactions, chunk_index) do |chunk_transactions, idx|
            # Сортировка транзакций
            sorted_transactions = merge_sort(chunk_transactions)
            # Запись отсортированных транзакций в временный файл
            temp_file_path = unique_filename(temp_dir)
            File.open(temp_file_path, 'w') do |temp_file|
              sorted_transactions.each do |t|
                temp_file.puts t.to_s
              end
            end
            # Добавляем путь к временному файлу
            mutex.synchronize { temp_files << temp_file_path }
            puts "Чанк #{idx}: Обработан и сохранён"
          end

          threads << thread

          # Ограничение количества одновременно работающих потоков
          if threads.size >= max_threads
            threads.each(&:join)
            threads.clear
          end

          chunk_index += 1
        end

        # Ожидание завершения оставшихся потоков
        threads.each(&:join)
      end
    end
    puts "Общее время создания и сортировки чанков: #{chunk_creation_time.round(4)} секунд"

    # Шаг 2: Слияние отсортированных чанков
    merge_time = Benchmark.realtime do
      merge_sorted_files(temp_files, output_file_path)
    end
    puts "Время слияния чанков: #{merge_time.round(4)} секунд"

    FileUtils.rm_rf(temp_dir)
    puts "Временные файлы удалены"
  end
  puts "Общее время выполнения: #{total_time.round(4)} секунд"
end


if __FILE__ == $0
  external_sort('input_transactions.txt', 'sorted_transactions.txt', CHUNK_SIZE_MB, MAX_THREADS)
end
