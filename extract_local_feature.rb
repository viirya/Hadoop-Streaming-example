#!/usr/bin/env ruby

STDIN.each_line do |line|
    # obtain filename from file list
    filename = line.chop!
    if /\A.*\/(.*?)\z/i.match(filename)
        localfilename = $1
    else
        exit
    end
    # copy file from HDFS to local disk
    system("/opt/hadoop/bin/hadoop dfs -get /user/viirya/" + filename + " " + localfilename)
    system("mkdir images")
    system("mv " + localfilename + " images/.")
    system("tar xfv images/" + localfilename + " -C images")
    system("chmod a+x ./extract_features_64bit.ln")

    files = Dir.glob("images/*")
    files.each do |image_file|
        #begin to process local file
        system("/usr/bin/convert " + image_file + " " + image_file + ".pgm")
        system("./extract_features_64bit.ln -hesaff -sift -i " + image_file + ".pgm -o1 " + image_file + ".hes")
        system("rm " + image_file)
        system("rm " + image_file + ".pgm")
        puts "0\t#{image_file}"
    end

    system("ls -al images/ > log.txt")
    system("/opt/hadoop/bin/hadoop dfs -put log.txt /user/viirya/output/gen_features/.")

    system("mkdir features")
    system("mv images/*.hes features/.")
    system("tar cf " + localfilename + " -C features .")
    system("/opt/hadoop/bin/hadoop dfs -put " + localfilename + " " + "/user/viirya/output/gen_features/.")

    puts "0\t#{filename}"
    puts "0\t#{localfilename}"
    system("rm " + localfilename)
    system("rm -rf images")
    system("rm -rf features")
end
