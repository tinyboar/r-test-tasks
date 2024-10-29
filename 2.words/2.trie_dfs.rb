class TrieNode
  attr_accessor :children, :is_end_of_word

  def initialize
    @children = {}
    @is_end_of_word = false
  end
end

class Trie
  def initialize
    @root = TrieNode.new
  end

  def insert(word)
    node = @root
    word.each_char do |char|
      node.children[char] ||= TrieNode.new
      node = node.children[char]
    end
    node.is_end_of_word = true
  end

  def search_prefix(prefix)
    node = @root
    prefix.each_char do |char|
      return nil unless node.children[char]
      node = node.children[char]
    end
    node
  end

  def search_word(word)
    node = search_prefix(word)
    node && node.is_end_of_word
  end
end

def word_break_trie(string, dictionary)
  trie = Trie.new
  dictionary.each { |word| trie.insert(word) }
  memo = {}

  dfs = lambda do |start|
    return true if start == string.length
    return memo[start] if memo.key?(start)

    node = trie.search_prefix("")
    (start...string.length).each do |end_index|
      char = string[end_index]
      node = node&.children[char]
      break unless node
      if node.is_end_of_word && dfs.call(end_index + 1)
        memo[start] = true
        return true
      end
    end

    memo[start] = false
    false
  end

  dfs.call(0)
end

string = "двесотни"
dictionary = ["две", "сотни", "тысячи"]
puts word_break_trie(string, dictionary)
