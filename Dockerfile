FROM ruby:2.6.5
RUN mkdir /myapp
WORKDIR /myapp
COPY Gemfile /myapp/Gemfile
COPY Gemfile.lock /myapp/Gemfile.lock
RUN bundle install
RUN bundler install --binstubs
COPY . /myapp
EXPOSE 3000