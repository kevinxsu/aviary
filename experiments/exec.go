package main 

import (
  "fmt"
  "os/exec"
  "os"
  "log"
  "path/filepath"
)

func main() {
  path := "../mrapps/wc.go"
  cmd := exec.Command("go", "build", "-buildmode=plugin", path, "-o asdfasdf")
  err := cmd.Run() 
  if err != nil {
    log.Fatal(err)
  }

  fmt.Println("wc.so created")

  out, err := exec.Command("ls").Output()
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(string(out))

  files, err := filepath.Glob("*.so")
  if err != nil {
    log.Fatal(err)
  }


  for _, f := range files {
    err = os.Remove(f)
    if err != nil {
      log.Fatal(err)
    }
  }
}
