Gem::Specification.new do |spec|
  spec.name = 'spectra_support'
  spec.version = '5.0.0'
  spec.summary = 'Spectra Support'
  spec.author = 'Robert Grimm <robg@spectralogic.com>'
  spec.require_paths = ['lib']
  # Process.clock_gettime requires 2.1+
  spec.required_ruby_version = '>= 2.1.0'
  spec.add_dependency('ffi')
  spec.add_dependency('open4', '>= 1.3.0')
  spec.add_development_dependency('minitest')
  spec.add_development_dependency('mocha', '>= 0.9.12')
  spec.files = Dir['lib/**/*.rb'] + Dir['libexec/**/*'] + Dir['test/**/*.rb'] + ['Rakefile']
end