Gem::Specification.new do |s|
  s.name = 'hivemeta'
  s.rubyforge_project = 'hivemeta'
  s.version = '0.0.5'
  s.date = '2011-05-19'
  s.authors = ["Frank Fejes"]
  s.email = 'frank@fejes.net'
  s.summary =
    'Use the hive metadb to write map/reduce and query table info.'
  s.homepage = 'https://github.com/fsfiii/hivemeta'
  s.description =
    'Use the hive metadb to write map/reduce and easily query table info.'
  s.files = [
    "README",
    "CHANGELOG",
    "lib/hivemeta.rb",
    "lib/hivemeta/connection.rb",
    "lib/hivemeta/record.rb",
    "lib/hivemeta/table.rb",
    "examples/hivemeta_query.rb",
    "examples/hivemeta_testrec.rb",
    "examples/sample-mapper.rb",
  ]
end
