FROM ruby:2.6.5-buster

# Install ruby gems
ADD Gemfile /app/Gemfile
ADD Gemfile.lock /app/Gemfile.lock
ADD ./lib/beetle_etl/version.rb /app/lib/beetle_etl/version.rb
ADD beetle_etl.gemspec /app/beetle_etl.gemspec
WORKDIR /app
RUN gem install bundler && bundle