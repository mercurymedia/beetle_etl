FROM ruby:2.6.5-buster

# Install ruby gems
ADD Gemfile /app/Gemfile
ADD Gemfile.lock /app/Gemfile.lock
ADD beetle_etl.gemspec /app/beetle_etl.gemspec
WORKDIR /app
RUN gem install bundler && bundle