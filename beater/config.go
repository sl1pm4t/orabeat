package orabeat

type OraConfig struct {
	Period   *int64
	Host     string `config:"host"`
	Port     uint16 `config:"port"`
	Service  string `config:"service"`
	User     string `config:"user"`
	Password string `config:"password"`
	Stats    struct {
		System  *bool `config:"system"`
		Session *bool `config:"session"`
	}
	StatsFilter string `config:"statfilter"`
}

type ConfigSettings struct {
	Input OraConfig
}
