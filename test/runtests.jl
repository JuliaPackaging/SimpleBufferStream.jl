using SimpleBufferStream, Test

@testset "basic usage" begin
    bs = BufferStream()
    @test write(bs, "hello") == 5
    @test String(readavailable(bs)) == "hello"

    @test write(bs, "world") == 5
    @test read(bs, UInt8) == codeunits("w")[1]
    @test String(readavailable(bs)) == "orld"

    t_read = @async begin
        t_elapsed = @elapsed begin
            data = String(readavailable(bs))
        end
        @test t_elapsed > 0.01
        return data
    end
    sleep(0.05)
    @test write(bs, "go") == 2
    @test fetch(t_read) == "go"

    # Test readbytes!()
    @test write(bs, "spinlocked") == 10
    buff = zeros(UInt8, 8)
    @test readbytes!(bs, buff, 4) == 4
    @test buff[1:4] == codeunits("spin")
    @test all(buff[5:8] .== 0)
    @test !eof(bs)
    @test length(bs) == 6
    @test mem_usage(bs) == 10
    @test readbytes!(bs, buff, 8) == 6
    @test buff[1:6] == codeunits("locked")
    @test all(buff[7:8] .== 0)
    @test length(bs) == 0
    @test mem_usage(bs) == 0

    t_cancel = @async begin
        @test readavailable(bs) == UInt8[]
    end
    close(bs)
    wait(t_cancel)

    # Test people who do read() a bunch:
    bs = BufferStream()
    @test write(bs, "Never ") == 6
    @test write(bs, "have ") == 5
    @test write(bs, "I ") == 2
    @test write(bs, "ever") == 4
    @test length(bs) == 17
    close(bs)

    output = UInt8[]
    while !eof(bs)
        push!(output, read(bs, UInt8))
    end
    @test output == codeunits("Never have I ever")

    bs = BufferStream()
    @test write(bs, "Never ") == 6
    @test write(bs, "have ") == 5
    @test write(bs, "I ") == 2
    @test write(bs, "ever") == 4
    close(bs)

    @test length(bs) == 17
    @test String(readavailable(bs)) == "Never "
    @test length(bs) == 11
    skip(bs, 7)
    @test length(bs) == 4
    @test String(readavailable(bs)) == "ever"
    @test length(bs) == 0
    @test eof(bs)

    # Test write(bs, ::UInt8)
    bs = BufferStream()
    @test write(bs, UInt8(1)) == 1
    close(bs)
    @test read(bs) == UInt8[1]
end

@testset "max_len" begin
    bs = BufferStream(1000)
    @test_throws ArgumentError write(bs, zeros(UInt8, 1001))
    write(bs, zeros(UInt8, 1000))
    t_write = @async begin
        t_elapsed = @elapsed write(bs, zeros(UInt8, 1))
        @test t_elapsed > 0.01
    end
    sleep(0.05)
    @test t_write.state == :runnable
    # Read a piece of that chunk, show that `max_len` is based off of memory usage, not bytes left to be read:
    buff = Array{UInt8}(undef, 100)
    readbytes!(bs, buff, 100)
    sleep(0.05)
    @test t_write.state == :runnable
    # Consume the rest of that chunk so it can be dropped, allowing the pending write to go through
    readavailable(bs)
    wait(t_write)
end

function tee_task(io_in, io_outs...)
    return @async begin
        total_size = 0
        while !eof(io_in)
            chunk = readavailable(io_in)
            total_size += length(chunk)
            for io_out in io_outs
                write(io_out, chunk)
            end
        end
        for io_out in io_outs
            close(io_out)
        end
    end
end

@testset "Tee'ing" begin
    mktemp() do fname, io_file
        input = BufferStream()
        output = BufferStream()
        t_tee = tee_task(input, output, io_file)

        write(input, "hello"^1000)
        write(input, "hello"^100)
        write(input, "hello"^10)
        close(input)

        wait(t_tee)
        @test filesize(fname) == 5*1110
        @test String(read(output)) == "hello"^1110
        @test String(read(fname)) == "hello"^1110
    end
end

function task_sleepy_chain(io_in, io_out; sleep_factor=0.1*rand())
    @async begin
        while !eof(io_in)
            write(io_out, readavailable(io_in))
            sleep(sleep_factor*rand())
        end
        close(io_out)
    end
end

function task_fixedsize_chain(io_in, io_out; buffsize=rand(128:512))
    @async begin
        buff = Vector{UInt8}(undef, buffsize)
        while !eof(io_in)
            r = readbytes!(io_in, buff, buffsize)
            @assert r <= buffsize
            write(io_out, buff[1:r])
        end
        close(io_out)
    end
end

@testset "async passing" begin
    # Sleepy chaos tests
    tasks = Task[]
    input_buff = BufferStream()
    last_buff = input_buff
    for idx in 1:9
        # Create a link in the chain of tasks that randomly sleeps
        sleepy_buff = BufferStream()
        push!(tasks, task_sleepy_chain(last_buff, sleepy_buff))

        # Create a link in the chain of tasks that only reads a fixed size
        fixed_buff = BufferStream()
        push!(tasks, task_fixedsize_chain(sleepy_buff, fixed_buff))
        last_buff = fixed_buff
    end

    # Generate a bunch of chunks that should flow down the chain:
    result = UInt8[]
    for idx in 1:10
        chunk = rand(UInt8, rand(64:64:4096))
        write(input_buff, chunk)
        append!(result, chunk)
    end
    close(input_buff)

    # Wait for the tasks to complete
    for idx in 1:length(tasks)
        wait(tasks[idx])
    end

    # Ensure that the last buffer gets everything perfectly.
    @test read(last_buff) == result
end

function task_compare(io_in, truth_io)
    @async begin
        idx = 0
        ret = UInt8[]
        while !eof(io_in)
            chunk = readavailable(io_in)
            append!(ret, chunk)

            truth_chunk = read(truth_io, length(chunk))
            if chunk != truth_chunk
                @error("Divergence in chunk starting at index $(idx)", chunk, truth_chunk)
            end
            idx += length(chunk)
        end
        return ret
    end
end

# I built this package so that it could work well with HTTP/Gzip/Tar, so let's make sure it does
using HTTP, Gzip_jll, Tar
@testset "HTTP.jl streaming" begin
    reg_uuid = "23338594-aafe-5451-b93e-139f81909106"
    reg_hash = "433c8c72652d3230287c72184da6be3325155b64"
    url = "https://us-east.storage.juliahub.com/registry/$(reg_uuid)/$(reg_hash)"

    mktemp() do file_path, file_io
        # Get our ground-truth value:
        @test HTTP.get(url, response_stream=file_io).status == 200

        # Ensure that streaming through a BufferStream gets the exact same thing
        http_buffio = BufferStream()
        t_compare = task_compare(http_buffio, open(file_path, "r"))
        @test HTTP.get(url, response_stream=http_buffio).status == 200
        @test fetch(t_compare) == read(file_path)

        # Ensure that streaming HTTP -> BufferStream -> Gzip works
        http_buffio = BufferStream()
        
        gzip_http = gzip(gz -> open(`$gz -d`, read=true, write=true))
        gzip_file = gzip(gz -> open(pipeline(`cat $(file_path)`, `$gz -d`), read=true))
        t_compare = task_compare(gzip_http.out, gzip_file.out)
        @test HTTP.get(url, response_stream=gzip_http.in).status == 200
        wait(gzip_http)
        wait(gzip_file)

        # Make another gzip_file for the final comparison.  :P
        gzip_file = gzip(gz -> open(pipeline(`cat $(file_path)`, `$gz -d`), read=true))
        @test fetch(t_compare) == read(gzip_file.out)
    end

    # Finally, test the literal `PkgServer.jl` workload
    mktemp() do file_path, file_io
        http_buffio = BufferStream()
        tar_skip_buffio = BufferStream()
        tar_noskip_buffio = BufferStream()

        # Create gzip process to decompress for us, using `gzip()` from `Gzip_jll`
        gzip_proc = gzip(gz -> open(`$gz -d`, read=true, write=true))

        # Create tee nodes, one http -> (file, gzip), and one gzip -> (skip, noskip)
        http_tee_task = tee_task(http_buffio, file_io, gzip_proc.in)
        tar_tee_task = tee_task(gzip_proc.out, tar_skip_buffio, tar_noskip_buffio)

        # Create two tasks to read in the gzip output and do in-tar tree hashing.
        tar_skip_task = @async Tar.tree_hash(tar_skip_buffio; skip_empty=true)
        tar_noskip_task = @async Tar.tree_hash(tar_noskip_buffio; skip_empty=false)

        # Start the HTTP response, ensure it's 200 OK
        @test HTTP.get(url, response_stream=http_buffio).status == 200

        # Wait for the async tasks to finish
        wait(http_tee_task)
        wait(tar_tee_task)

        # Fetch the result of the tarball hash checks
        calc_skip_hash = fetch(tar_skip_task)
        calc_noskip_hash = fetch(tar_noskip_task)
        @test calc_skip_hash == calc_noskip_hash == reg_hash
    end
end
