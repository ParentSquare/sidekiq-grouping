appraise 'sidekiq-4.0' do
  gem 'sidekiq', '~> 4.0.0'
end

appraise 'sidekiq-4.1' do
  gem 'sidekiq', '~> 4.1.0'
end

appraise 'sidekiq-4.2' do
  gem 'sidekiq', '~> 4.2.0'
end

appraise 'sidekiq-5.0' do
  gem 'sidekiq', '~> 5.0.0'
end

appraise 'sidekiq-6.0' do
  gem 'sidekiq', '~> 6.0.0'
end

appraise 'sidekiq-6.5' do
  gem 'sidekiq', '~> 6.5.0'
end

appraise 'sidekiq-7.0' do
  gem 'sidekiq', '~> 7.0.0'
end

# Sidekiq main now depends on connection_pool 3, which needs Ruby >= 3.2.
if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.2')
  appraise 'sidekiq-master' do
    gem 'sidekiq', github: 'mperham/sidekiq', branch: 'main'
  end
end
