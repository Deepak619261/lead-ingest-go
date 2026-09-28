// gencsv writes a deterministic lead CSV with injected duplicate/invalid/malformed rows.
package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
)

var header = []string{
	"First Name", "Last Name", "Email", "Phone", "Company",
	"Title", "LinkedIn", "City", "Country", "Industry",
}

var (
	firstNames = []string{"Aarav", "Ananya", "John", "Maria", "Wei", "Fatima", "Liam", "Olivia", "Rohan", "Sofia", "Deepak", "Priya"}
	lastNames  = []string{"Sharma", "Smith", "Garcia", "Chen", "Khan", "Brown", "Iyer", "Müller", "O'Neil", "Rossi"}
	companies  = []string{"Acme Inc", "Globex", "Initech", "Umbrella, Ltd", "Stark Industries", "Wayne Enterprises", "Hooli"}
	titles     = []string{"VP Sales", "SDR", "Head of Growth", "CTO", "Account Executive", "Founder"}
	cities     = []string{"Bengaluru", "Hyderabad", "San Francisco", "London", "Berlin", "Austin"}
	countries  = []string{"IN", "US", "GB", "DE"}
	industries = []string{"SaaS", "Fintech", "Healthcare", "Retail", "Manufacturing"}
)

func main() {
	var (
		n         = flag.Int("n", 10_000, "number of data rows")
		out       = flag.String("out", "leads.csv", "output file")
		seed      = flag.Uint64("seed", 42, "random seed (same seed => same file)")
		dupPct    = flag.Float64("dup", 0.02, "fraction of rows that repeat an earlier email")
		invalid   = flag.Float64("invalid", 0.03, "fraction of rows that fail validation")
		malformed = flag.Float64("malformed", 0.001, "fraction of rows with wrong column count")
	)
	flag.Parse()

	if err := run(*out, *n, *seed, *dupPct, *invalid, *malformed); err != nil {
		log.Fatal(err)
	}
}

func run(path string, n int, seed uint64, dupPct, invalidPct, malformedPct float64) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	// Named return + defer: if Close fails (e.g. disk full on flush) we report it.
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()

	bw := bufio.NewWriterSize(f, 1<<20) // 1 MiB buffer => few syscalls
	w := csv.NewWriter(bw)

	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	pick := func(xs []string) string { return xs[rng.IntN(len(xs))] }

	if err := w.Write(header); err != nil {
		return err
	}

	// A small window of recent emails makes realistic duplicates without holding every email.
	recent := make([]string, 0, 1024)
	var stats struct{ dup, invalid, malformed int }

	for i := 0; i < n; i++ {
		first, last := pick(firstNames), pick(lastNames)
		company := pick(companies)
		email := fmt.Sprintf("%s.%s.%d@%s.com", slug(first), slug(last), i, slug(strings.Fields(company)[0]))
		phone := fmt.Sprintf("+1415%07d", rng.IntN(10_000_000))
		linkedin := fmt.Sprintf("https://www.linkedin.com/in/%s-%s-%d", slug(first), slug(last), i)

		r := rng.Float64()
		switch {
		case r < malformedPct:
			stats.malformed++
			// Too few columns: the reader must report this row and continue.
			if err := w.Write([]string{first, last, email}); err != nil {
				return err
			}
			continue
		case r < malformedPct+invalidPct:
			stats.invalid++
			switch rng.IntN(4) {
			case 0:
				email = strings.Replace(email, "@", ".at.", 1) // bad format
			case 1:
				email, phone, linkedin = "", "", "" // no identifier at all
			case 2:
				email = "noreply@" + strings.Split(email, "@")[1] // arbitrary mailbox
			case 3:
				first = strings.Repeat("x", 300) // exceeds 250 limit
			}
		case r < malformedPct+invalidPct+dupPct && len(recent) > 0:
			stats.dup++
			email = strings.ToUpper(recent[rng.IntN(len(recent))]) // case differs: dedupe must lower()
		}

		if email != "" && len(recent) < cap(recent) {
			recent = append(recent, email)
		} else if email != "" {
			recent[rng.IntN(len(recent))] = email
		}

		rec := []string{first, last, email, phone, company, pick(titles), linkedin, pick(cities), pick(countries), pick(industries)}
		if err := w.Write(rec); err != nil {
			return err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %d rows to %s (≈dup %d, invalid %d, malformed %d)\n",
		n, path, stats.dup, stats.invalid, stats.malformed)
	return nil
}

// slug keeps a-z and 0-9 so generated emails are valid ASCII ("Müller" -> "mller", "Umbrella," -> "umbrella").
func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
