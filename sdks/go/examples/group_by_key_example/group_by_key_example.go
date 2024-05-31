package main

import (
	"context"
	"flag"
	"strings"
	"strconv"
	//"time"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"log/slog"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
        output = flag.String("output_path", "gs://priyans-testing-bucket", "The output file path.")
)

func init() {
        register.Function3x0(mappingFn)
}

func format(k string, vs func(*string) bool, emit func(string)) {
         var v string
//       if (strings.Contains(k, "2")|| strings.Contains(k, "4") || strings.Contains(k, "6") || strings.Contains(k, "1")) {
        for vs(&v) {
          emit(k + v)
        }
//       } else if (strings.Contains(k, "5") || strings.Contains(k, "7")) {
//             count := 0
//             for vs(&v) {
//               if (count == 0 || count == 2) {
//                  time.Sleep(60 * time.Second)
//                   emit(k + v)
//               } else {
//                  emit(k + v)
//               }
//               count += 1
//
//             }
//       }
         slog.Error("Done with " + k)
}

func mappingFn(ctx context.Context, key string, emit func(string, string)) {
        constants := []string{
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
                "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                "x", "y", "z", "0", "1", "2", "3", "4", "5", "6", "7",
                "8", "9", ":", ";", "<", ">", ".", "/", "?", "@", "$",
                "%", "^", "!", "-", "+", "=", "}", "{", "[", "]", "(",
        }

        for i := 0; i < 10; i++ {
                chars := strings.Repeat(constants[i], 2000000)
                str := strconv.Itoa(i) + "-" + chars + "-" + key + "-" + strconv.Itoa(i)
                emit(key, str)
        }
}

func main() {
	flag.Parse()
        beam.Init()

        ctx := context.Background()

        p := beam.NewPipeline()
        s := p.Root()

        keys := []string{
                "Problematic HotKey 1",
                "Problematic HotKey 2",
                "Problematic HotKey 3",
                "Problematic HotKey 4",
                "Problematic HotKey 5",
                "Problematic HotKey 6",
                "Problematic HotKey 7",
                "Problematic HotKey 8",
                "Problematic HotKey 9",
                "Problematic HotKey 10"}
        keysPcol := beam.CreateList(s, keys)

        kvPCol := beam.ParDo(s, mappingFn, keysPcol)
        kvs := beam.GroupByKey(s, kvPCol)
        _ = beam.ParDo(s, format, kvs)
        // textio.Write(s, *output, out)

        if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}