module.exports = {
	concurrency: 10,
	mysql: {
		host: '',
		user: '',
		password: '',
		database: ''
	},
	es: {
		host: '',
		index: 'item_fulltext_index_write',
		type: 'item_fulltext'
	},
	s3: {
		params: {
			Bucket: ''
		},
		accessKeyId: '',
		secretAccessKey: ''
	}
};
