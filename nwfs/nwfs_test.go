package nwfs

import (
	"context"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestFs_Watch(t *testing.T) {
	// Create random directory
	dir := "./TestFs_Watch-" + strconv.Itoa(rand.Intn(math.MaxInt-1))
	fs := NewFs(dir, []string{}, mockLogger{})
	baseTime := time.Now().UTC()

	type args struct {
		chanFiles chan NewFile
	}
	tests := []struct {
		name             string
		preexistingFiles []NewFile
		newFiles         []NewFile
		expected         []NewFile
		expectedErr      error
	}{
		{
			name: "preexisting files",
			preexistingFiles: []NewFile{
				mockFile(fs, "1", baseTime.Add(-time.Hour)),
				mockFile(fs, "2", baseTime.Add(-time.Hour)),
			},
			newFiles: []NewFile{},
			expected: []NewFile{
				mockFile(fs, "1", baseTime.Add(-time.Hour)),
				mockFile(fs, "2", baseTime.Add(-time.Hour)),
			},
			expectedErr: nil,
		},
		{
			name:             "new files",
			preexistingFiles: []NewFile{},
			newFiles: []NewFile{
				mockFile(fs, "3", baseTime.Add(-time.Hour)),
				mockFile(fs, "4", baseTime.Add(-time.Hour)),
			},
			expected: []NewFile{
				mockFile(fs, "3", baseTime.Add(-time.Hour)),
				mockFile(fs, "4", baseTime.Add(-time.Hour)),
			},
			expectedErr: nil,
		},
		{
			name:             "nested files",
			preexistingFiles: []NewFile{},
			newFiles: []NewFile{
				mockFile(fs, "nested/3", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested/4", baseTime.Add(-time.Hour)),
			},
			expected: []NewFile{
				mockFile(fs, "nested/3", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested/4", baseTime.Add(-time.Hour)),
			},
			expectedErr: nil,
		},
		{
			name:             "deeper nested files",
			preexistingFiles: []NewFile{},
			newFiles: []NewFile{
				mockFile(fs, "3", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested1/4", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested2/nested3/4", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested3/nested4/5", baseTime.Add(-time.Hour)),
			},
			expected: []NewFile{
				mockFile(fs, "3", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested1/4", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested2/nested3/4", baseTime.Add(-time.Hour)),
				mockFile(fs, "nested3/nested4/5", baseTime.Add(-time.Hour)),
			},
			expectedErr: nil,
		},
		{
			name: "ignore preexisting files",
			preexistingFiles: []NewFile{
				mockFile(fs, "unprocessable/3", baseTime.Add(-time.Hour)),
				mockFile(fs, "redirect/4", baseTime.Add(-time.Hour)),
			},
			expected:    []NewFile{},
			expectedErr: nil,
		},
		{
			name:             "ignore new files",
			preexistingFiles: []NewFile{},
			newFiles: []NewFile{
				mockFile(fs, "unprocessable/3", baseTime.Add(-time.Hour)),
				mockFile(fs, "redirect/4", baseTime.Add(-time.Hour)),
			},
			expected:    []NewFile{},
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.MkdirAll(fs.dir, 0755)
			defer os.RemoveAll(fs.dir)
			for _, file := range tt.preexistingFiles {
				err := os.MkdirAll(filepath.Dir(file.Path), 0755)
				if err != nil {
					t.Fatal(err)
				}

				f, err := os.OpenFile(file.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					t.Fatal(err)
				}

				_, err = f.Write(file.Bytes)
				if err != nil {
					t.Fatal(err)
				}

				err = os.Chtimes(file.Path, file.ReceivedTime, file.ReceivedTime)
				if err != nil {
					t.Fatal(err)
				}

				err = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}

			chanFiles := make(chan NewFile)
			chanErr := make(chan error)
			go func() {
				err := fs.Watch(context.Background(), chanFiles)
				if err != nil {
					chanErr <- err
				}
			}()

			time.Sleep(time.Second)
			for _, file := range tt.newFiles {
				err := os.MkdirAll(filepath.Dir(file.Path), 0755)
				if err != nil {
					t.Fatal(err)
				}

				f, err := os.OpenFile(file.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					t.Fatal(err)
				}

				_, err = f.Write(file.Bytes)
				if err != nil {
					t.Fatal(err)
				}

				err = os.Chtimes(file.Path, file.ReceivedTime, file.ReceivedTime)
				if err != nil {
					t.Fatal(err)
				}

				err = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}

			for {
				select {
				case actualFile := <-chanFiles:
					var expectedFile *NewFile
					for i, file := range tt.expected {
						if file.Name == actualFile.Name {
							expectedFile = &file
							tt.expected = slices.Delete(tt.expected, i, i+1)
							break
						}
					}

					if expectedFile == nil {
						t.Fatalf("unexpected file %s", actualFile.Name)
					}

					if actualFile.Name != expectedFile.Name {
						t.Fatalf("expected file name %s, got %s", expectedFile.Name, actualFile.Name)
					}
					if string(actualFile.Bytes) != string(expectedFile.Bytes) {
						t.Fatalf("expected file bytes %s, got %s", expectedFile.Bytes, actualFile.Bytes)
					}

					if delta := expectedFile.ReceivedTime.Sub(actualFile.ReceivedTime); delta > time.Millisecond*50 || delta < -time.Millisecond*50 {
						t.Fatalf("expected file received time not within expected error %s, got %s", expectedFile.ReceivedTime, actualFile.ReceivedTime)
					}

					if len(tt.expected) == 0 {
						return
					}
				case err := <-chanErr:
					t.Fatal(err)
				case <-time.After(time.Millisecond * 700):
					if len(tt.expected) == 0 {
						return
					}
					t.Fatalf("timed out waiting for files")
				}
			}
		})
	}
}

func mockFile(fs Fs, relativePath string, receivedTime ...time.Time) NewFile {
	nf := NewFile{
		Name:  filepath.Base(relativePath),
		Path:  path.Join(fs.dir, relativePath),
		Bytes: []byte(filepath.Base(relativePath)),
	}

	if len(receivedTime) > 0 {
		nf.ReceivedTime = receivedTime[0].UTC()
	}

	return nf
}
