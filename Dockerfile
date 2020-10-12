FROM ruby:2.6.5
RUN mkdir /myapp
WORKDIR /myapp
COPY Gemfile /myapp/Gemfile
COPY Gemfile.lock /myapp/Gemfile.lock
#ENV GEM_HOME="/usr/local/bundle"
#ENV PATH $GEM_HOME/bin:$GEM_HOME/gems/bin:$PATH
#COPY docker-entrypoint.sh /usr/local/bin/
#RUN chmod 777 /usr/local/bin/docker-entrypoint.sh \
#    && ln -s /usr/local/bin/docker-entrypoint.sh /
RUN bundle install
#RUN bundle install --binstubs
COPY . /myapp
#RUN chmod +x docker-entrypoint.sh
#ENTRYPOINT ["docker-entrypoint.sh"]
EXPOSE 3000