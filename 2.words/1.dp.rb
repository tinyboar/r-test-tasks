require 'set'

def word_break(string, dictionary)
  return false if string.empty?

  word_set = dictionary.to_set
  max_word_length = dictionary.map(&:length).max || 0
  dp = Array.new(string.length + 1, false)
  dp[0] = true

  (1..string.length).each do |i|
    (1..[i, max_word_length].min).each do |l|
      if dp[i - l] && word_set.include?(string[i - l...i])
        dp[i] = true
        break
      end
    end
  end

  dp[string.length]
end

string = "двесотнидвести"
dictionary = ["две", "сотни", "тысячи", "двести"]
puts word_break(string, dictionary)
