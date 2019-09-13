package main

// fast
//
//
//
// Inputs: 	[1] XML file, dataset to mask
//			[2] XLS file, all values to be replaced WITHOUT HEADERS
//			[3] XLS file, seed values (first character of input -:> output)
//			[4] string, filename of the output xml

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tealeg/xlsx"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

const (
	A9                     = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // String of possible chars for random string generator
	letterIdxBits          = 6                                      // 6 bits to represent a letter index
	specialCharactersRegex = "[<>&'\"]"                             // These characters get converted in XML, so we need to split around them
	letterIdxMask          = 1<<letterIdxBits - 1                   // All 1-bits, as many as letterIdxBits
	letterIdxMax           = 63 / letterIdxBits                     // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())
var maskIntDelimiter = 77     // Surround masked ints with 77 on either side
var smallestMaskingAmount = 2 // Don't mask chunks smaller than 2 characters

type maskstring struct {
	value  string
	masked bool
}

func initCmdLineArgs() (string, string, string, string) {
	if len(os.Args) != 5 {
		_, _ = fmt.Printf("Invalid number of args. Need 4, got %d\n", len(os.Args)-1)
		_, _ = fmt.Println("example usage: > masking dataset.xml datatomask.xlsx seeds.xlsx outputname.xml ")
		os.Exit(3)
	}
	datasetFilename := os.Args[1]
	valuesToReplaceFilename := os.Args[2]
	seedFilename := os.Args[3]
	outputFilename := os.Args[4]
	return datasetFilename, valuesToReplaceFilename, seedFilename, outputFilename
}

func initSeeds(seedFilepath string) map[string]string {
	seedXls, err := xlsx.OpenFile(seedFilepath)
	if err != nil {
		print(err.Error())
		os.Exit(45)
	}
	seedSheet := seedXls.Sheets[0]
	seeds := make(map[string]string)
	seedRows := seedSheet.Rows
	for i := 0; i < len(seedSheet.Rows); i++ {
		key := seedRows[i].Cells[0].Value
		value := seedRows[i].Cells[1].Value

		seeds[key] = value
	}
	return seeds
}

// Generate a new string based on seed and counter. Allows for some control (so that not every ID is just completely random)
func generateNewValue(seed string, counter int, desiredLength int) string {
	genString := ""
	if desiredLength > smallestMaskingAmount {
		genString = strings.Join([]string{seed, strconv.Itoa(counter), "M"}, "")
	} else { // still unique, just shorter
		genString = generateUnique4CharString(counter)
	}
	return genString
}

func importMappedValuesFromFile(excelFilepath string) (map[string][]maskstring, error) {
	valuesXls, err := xlsx.OpenFile(excelFilepath)
	if err != nil {
		return nil, err
	}
	valuesSheet := valuesXls.Sheets[0]
	maskValuesInterim := make(map[string][]maskstring)
	valuesRows := valuesSheet.Rows
	descriptionRegex := regexp.MustCompile(specialCharactersRegex)

	// Initialize by setting keys and values equal so we can mask strings piece by piece
	for _, row := range valuesRows {
		for _, cell := range row.Cells {
			old := cell.Value
			if len(old) >= smallestMaskingAmount || !(len(maskValuesInterim[old]) > 0) {
				splitold := descriptionRegex.Split(old, -1)
				if len(splitold) == 1 {
					maskValuesInterim[old] = []maskstring{{old, false}}
				} else {
					for _, val := range splitold {
						if len(val) > smallestMaskingAmount {
							maskValuesInterim[val] = []maskstring{{val, false}}
						}
					}
				}
			}
		}
	}

	return maskValuesInterim, nil
}

func initMaskValues(maskValuesInterim map[string][]maskstring, seedmap map[string]string) map[string]string {

	maskValuesResolved := make(map[string]string)

	//Iterate from smallest to largest
	var sortedKeys []string
	for k := range maskValuesInterim {
		sortedKeys = append(sortedKeys, k)
	}
	sort.SliceStable(sortedKeys, func(i, j int) bool {
		return len(sortedKeys[i]) < len(sortedKeys[j])
	})

	counter := 1000

	for i, keyVal := range sortedKeys {
		// As we mask strings, this holds the strings which the keyValue will be mapped to in the end
		var stringsToJoin []string

		// If it's an int, mask it as one piece and pass
		if isInt(keyVal) {
			newString := strconv.Itoa(maskIntDelimiter) + strconv.Itoa(counter) + strconv.Itoa(maskIntDelimiter)
			maskValuesInterim[keyVal] = []maskstring{{newString, true}}
			stringsToJoin = []string{newString}
			counter++
		} else {
			//Mask the values of the current keyval
			for z, mstring := range maskValuesInterim[keyVal] {
				if !mstring.masked && len(mstring.value) > 0 {
					//check that string isn't some character we don't want to mask, or too short
					seedchar := string(mstring.value[0])
					seed := seedmap[seedchar]
					newValue := generateNewValue(seed, counter, len(mstring.value))
					maskValuesInterim[keyVal][z].value = newValue
					maskValuesInterim[keyVal][z].masked = true
					counter++
				}
				stringsToJoin = append(stringsToJoin, maskValuesInterim[keyVal][z].value)
			}
		}

		maskValuesResolved[keyVal] = strings.Join(stringsToJoin, "")

		// loop through all future keyvals and change the vals to what we want
		for _, afterKeyVal := range sortedKeys[i+1:] {
			var newArray []maskstring
			for z, mstring := range maskValuesInterim[afterKeyVal] {
				if strings.Contains(mstring.value, keyVal) {
					//Split into 3 slices, middle part contains value that we found
					valueToSplit := maskValuesInterim[afterKeyVal][z].value
					start := strings.Index(valueToSplit, keyVal)
					end := start + len(keyVal)
					firstPart := maskstring{valueToSplit[:start], false}     // what came before
					middlePart := maskstring{valueToSplit[start:end], false} // string to replace
					endPart := maskstring{valueToSplit[end:], false}         // what comes after

					// now change middlepart's value to the masked value of what we found
					middlePart.value = maskValuesResolved[keyVal]
					middlePart.masked = true

					//append to newArray
					newArray = append(newArray, firstPart, middlePart, endPart)
				} else {
					newArray = append(newArray, maskValuesInterim[afterKeyVal][z])
				}
			}
			maskValuesInterim[afterKeyVal] = newArray
		}
		counter++
	}
	return maskValuesResolved
}

// Iterates through dataset xml, writes each edited line to a new file
func maskDataset(datasetFilepath string, maskedValues map[string]string, outputFilename string) int {
	maskingStart := time.Now()
	fileLength, _ := getFileLength(datasetFilepath)
	_, _ = fmt.Printf("The file is %d lines long\n", fileLength)
	bar := initProgressBar(fileLength)
	onePercent := fileLength / 100

	dataset, _ := os.Open(datasetFilepath)
	reader := bufio.NewReader(dataset)
	output, _ := os.Create(outputFilename)
	writer := bufio.NewWriter(output)
	counter := 0
	var fromTokens = make([]string, len(maskedValues))
	var toTokens = make([]string, len(maskedValues))
	var i = 0
	for k := range maskedValues {
		fromTokens[i] = k
		i++
	}
	//sort from tokens longest to shortest, then populate toTokens with values from those keys
	sort.SliceStable(fromTokens, func(i, j int) bool {
		return len(fromTokens[i]) > len(fromTokens[j])
	})

	for i, key := range fromTokens {
		toTokens[i] = maskedValues[key]
		i++
	}

	keyValPairs := zip(fromTokens, toTokens)

	replacer := strings.NewReplacer(keyValPairs...)
	// mask any xml line with Description specifically
	descriptionRegex, _ := regexp.Compile("<Description>(.*)</Description>")

	for {
		line, _, err := reader.ReadLine()
		linetext := string(line)
		newline := descriptionRegex.ReplaceAllString(linetext, "<Description>MASKED_DESCRIPTION</Description>")
		newline = replacer.Replace(newline)
		_, _ = writer.WriteString(newline + "\n")

		if err == io.EOF {

			break
		}
		counter++

		// Every one percent, update the progress bar
		if counter%onePercent == 0 {
			bar.IncrBy(onePercent, time.Since(maskingStart))
		}

		if fileLength-counter == 3 {
			bar.Completed()
		}

		// Print out the last two lines
		if fileLength-counter < 2 {
			fmt.Println(newline)
		}
	}
	_ = writer.Flush()
	return counter
}

// Quick count of # of lines in a file [imported]
func getFileLength(filepath string) (int, error) {
	r, _ := os.Open(filepath)
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func main() {
	start := time.Now() // For timing
	datasetFilepath, valuesFilepath, seedPath, outputFilename := initCmdLineArgs()
	println("Command line arguments parsed...")
	seeds := initSeeds(seedPath) // Convert seeds into a key-value map of first character to seed value
	println("Seeds initialized...")
	maskedValuesInit, err := importMappedValuesFromFile(valuesFilepath)
	if err != nil {
		logrus.Error(err)
	}
	maskedValues := initMaskValues(maskedValuesInit, seeds) // Generate the masked value for each value needed, in a map
	err = printToFileMap("maskedValuesOUT.txt", maskedValues)
	if err != nil {
		logrus.Error(err)
	}
	println("Masked values generated...")
	count := maskDataset(datasetFilepath, maskedValues, outputFilename) // Iterate through the dataset xml and find/replace each old with each masked
	end := time.Since(start)
	time.Sleep(time.Second)
	println("Masking complete, processed", count, "lines")
	println("Output in:", outputFilename)
	println("Took", formatDuration(end))
}

// format duration to m:ss.ms
func formatDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	m := d / time.Minute
	if m > 0 {
		d -= m * time.Minute
	}
	s := d / time.Second
	d -= s * time.Second
	ms := d / time.Millisecond
	outstring := ""
	if m > 0 {
		outstring = fmt.Sprintf("%dm%02d.%04ds", m, s, ms)
	} else {
		outstring = fmt.Sprintf("%d.%04ds", s, ms)
	}
	return outstring
}

// console progress bar to show percentage complete
func initProgressBar(fileLength int) *mpb.Bar {
	p := mpb.New()
	total := int64(fileLength)
	name := "Masking Progress:"

	bar := p.AddBar(total,
		mpb.BarStyle("[██░]"),
		mpb.PrependDecorators(
			// display our name with one space on the right
			decor.Name(name, decor.WC{W: len(name) + 1, C: decor.DidentRight}),
			// replace ETA decorator with "done" message, OnComplete event
			decor.Elapsed(decor.ET_STYLE_GO),
		),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncSpace)),
	)
	return bar
}

// zip two arrays
// zip([a, b, c], [x, y, z]) -> [a, x, b, y, c, z]
func zip(a1, a2 []string) []string {
	r := make([]string, 2*len(a1))
	for i, e := range a1 {
		r[i*2] = e
		r[i*2+1] = a2[i]
	}
	return r
}

func isInt(val string) bool {
	_, err := strconv.Atoi(val)
	return err == nil
}

func printMapString(maskVals map[string]string) {
	for key, val := range maskVals {
		println(key, ":", val)
	}
}

// print a string array to file line by line
func printToFile(filename string, values []string) error {
	output, _ := os.Create(filename)
	writer := bufio.NewWriter(output)
	for _, val := range values {
		_, err := writer.WriteString(val + "\r\n")
		if err != nil {
			return err
		}
	}
	err := writer.Flush()
	return err
}

//print a map string -> string to file line by line like: "key: value"
func printToFileMap(filename string, values map[string]string) error {
	output, _ := os.Create(filename)
	writer := bufio.NewWriter(output)
	var sortedKeys []string
	for k := range values {
		sortedKeys = append(sortedKeys, k)
	}
	sort.SliceStable(sortedKeys, func(i, j int) bool {
		return len(sortedKeys[i]) < len(sortedKeys[j])
	})

	for _, key := range sortedKeys {
		_, err := writer.WriteString(key + ": " + values[key] + "\r\n")
		if err != nil {
			return err
		}
	}
	err := writer.Flush()
	return err
}

// Generates a unique 4-char string for any value less than 10000. Just a nice to have to not have all mask values be incredibly long
func generateUnique4CharString(seed int) string {
	A := int('A')
	numCharsInAlphabet := 52

	val1 := (seed % numCharsInAlphabet) + A
	leftover := seed / numCharsInAlphabet
	val2 := (leftover % numCharsInAlphabet) + A
	leftover = val2 / numCharsInAlphabet //Maximum of 10000/52/52 = 3.7 = 3
	char1 := string(val1)
	char2 := string(val2)
	char3 := "-"
	switch leftover {
	case 1:
		char3 = "_"
	case 2:
		char3 = "~"
	case 3:
		char3 = "-"
	case 4:
		char3 = "&"
	case 5:
		char3 = "~"
	case 6:
		char3 = "]"
	case 7:
		char3 = "#"
	case 8:
		char3 = "}"
	case 9:
		char3 = ","
	}

	output := char3 + char1 + char2 + char3

	return output
}
