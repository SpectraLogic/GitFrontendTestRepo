require 'tmpdir'
require 'open4'

def kill_postgres
end

Dir.mktmpdir {|dir|

  `killall postgres`

  `chown postgres #{dir}`
  return -1 unless $?.to_i == 0
  `su postgres -c 'initdb #{dir}'`
  return -1 unless $?.to_i == 0

  pid, stdin, stdout, stderr = Open4::popen4 "su postgres -c 'postgres -D #{dir}' &"
  #Allow postgres to start up
  sleep 5
  begin
    `su postgres -c 'createuser -sdr Administrator'`
    if $?.to_i != 0
      `kill #{pid}`
      return -1
    end
     
    IO.popen("/bin/sh #{File.expand_path('../safelyPackage.sh', __FILE__)}") do |io|
      while line = io.gets
        puts line
      end
      io.close
      if $?.to_i != 0
        puts "BUILD FAILED"
        exit -1
      end
    end
  ensure
    `killall postgres`
  end

}
