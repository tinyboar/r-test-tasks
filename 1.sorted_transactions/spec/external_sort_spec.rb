require 'rspec'
require 'tempfile'
require_relative '../external_sort'

module ExternalSortHelpers
  def create_temp_file(lines = [])
    file = Tempfile.new('external_sort_test')
    lines.each { |line| file.puts(line) }
    file.rewind
    file
  end

  def read_file_lines(file)
    File.readlines(file.path).map(&:chomp)
  end

  def read_file_content(file)
    File.read(file.path)
  end
end

RSpec.describe 'External Sort' do
  include ExternalSortHelpers

  after(:each) do
    input.close
    input.unlink
    output.close
    output.unlink
  end

  describe 'Sorting transactions' do
    let(:input) do
      create_temp_file([
        "2023-09-03T12:45:00Z,txn1,user1,100.00",
        "2023-09-03T12:46:00Z,txn2,user2,200.00",
        "2023-09-03T12:47:00Z,txn3,user3,150.00",
        "2023-09-03T12:48:00Z,txn4,user4,250.00",
        "2023-09-03T12:49:00Z,txn5,user5,50.00"
      ])
    end

    let(:output) { Tempfile.new('sorted_transactions') }

    it 'sorts transactions in descending order by amount' do
      expected = [
        "2023-09-03T12:48:00Z,txn4,user4,250.00",
        "2023-09-03T12:46:00Z,txn2,user2,200.00",
        "2023-09-03T12:47:00Z,txn3,user3,150.00",
        "2023-09-03T12:45:00Z,txn1,user1,100.00",
        "2023-09-03T12:49:00Z,txn5,user5,50.00"
      ]

      external_sort(input.path, output.path, 1, 1) # Размер чанка 1 МБ, 1 поток
      sorted_output = read_file_lines(output)

      expect(sorted_output).to eq(expected)
    end
  end

  describe 'Handling edge cases' do
    context 'when input file is empty' do
      let(:input) { create_temp_file }
      let(:output) { Tempfile.new('empty_output') }

      it 'produces an empty output file' do
        external_sort(input.path, output.path, 1, 1)
        expect(File.size(output.path)).to eq(0)
      end
    end

    context 'when input file has a single transaction' do
      let(:input) { create_temp_file(["2023-09-03T12:45:00Z,txn1,user1,100.00"]) }
      let(:output) { Tempfile.new('single_output') }

      it 'outputs the single transaction unchanged' do
        external_sort(input.path, output.path, 1, 1)
        sorted_output = read_file_content(output)
        expected = "2023-09-03T12:45:00Z,txn1,user1,100.00\n"

        expect(sorted_output).to eq(expected)
      end
    end
  end
end
