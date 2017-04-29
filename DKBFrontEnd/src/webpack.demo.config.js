var webpack = require('webpack');
var path = require('path');

var CommonsChunkPlugin = require("webpack/lib/optimize/CommonsChunkPlugin");
var HtmlWebpackPlugin = require('html-webpack-plugin');

var npmDir = path.join(__dirname, 'node_modules');

var webPrefix = process.env.WEB_PREFIX;
if (!Boolean(webPrefix)) {
    webPrefix = '';
}

if (webPrefix.length == 0) {
    webPrefix= '/';
} else if(webPrefix.charAt(webPrefix.length - 1) != '/') {
    webPrefix+= '/';
}


var graphLimit = process.env.RESTRICT_GRAPH;
var graphLimitEnd = '';
if (!Boolean(graphLimit)) {
    graphLimit = graphLimitEnd = '';
} else {
    graphLimit = `GRAPH <${graphLimit}> {`;
    graphLimitEnd = '}';
}

module.exports = {
    entry: {
        sparql: path.join(__dirname, 'src', 'examples', 'sparql.ts'),
        sparqlNoStats: path.join(__dirname, 'src', 'examples', 'sparqlNoStats.ts'),
        sparqlConstruct: path.join(__dirname, 'src', 'examples', 'sparqlConstruct.ts'),
        sparqlRDFGraph: path.join(__dirname, 'src', 'examples', 'sparqlRDFGraph.ts'),
        styleCustomization: path.join(__dirname, 'src', 'examples', 'styleCustomization.ts'),
    },
    resolve: {
        extensions: ['', '.ts', '.tsx', '.webpack.js', '.web.js', '.js'],
        alias: {
            // Backbone provided by joint.js, to prevent module duplication which
            // causes errors when Ontodia uses Backbone models from joint.js
            'backbone': path.join(npmDir, 'backbone', 'backbone.js'),
            // awful and temporary workaround to reference browser bundle instead of node's, see:
            // https://github.com/wycats/handlebars.js/issues/1102
            'handlebars': path.join(npmDir, 'handlebars', 'dist', 'handlebars.min.js'),
        },
    },
    module: {
        preLoaders: [
            {
                test: /\.ts$/,
                loader: 'string-replace',
                query: {
                    multiple: [
                        { search: '__SPARQL_ENDPOINT__',
                          replace: "'" + webPrefix + 'sparql-endpoint' + "'",
                          flags: 'g' },
                        { search: '__GRAPH_LIMIT_BEGIN__',
                          replace: graphLimit,
                          flags: 'g' },
                        { search: '__GRAPH_LIMIT_END__',
                          replace: graphLimitEnd,
                          flags: 'g' },
                    ]
                }
            },
        ],
        loaders: [
            {test: /\.ts$|\.tsx$/, loader: 'ts-loader'},
            {test: /\.css$/, loader: 'style-loader!css-loader'},
            {test: /\.scss$/, loader: 'style-loader!css-loader!sass-loader'},
            {test: /\.jpe?g$/, loader: 'url-loader?mimetype=image/jpeg'},
            {test: /\.gif$/, loader: 'url-loader?mimetype=image/gif'},
            {test: /\.png$/, loader: 'url-loader?mimetype=image/png'},
        ],
    },
    plugins: [
        new HtmlWebpackPlugin({
            title: 'Ontodia SparQL Demo',
            chunks: ['commons', 'sparql'],
            template: path.join(__dirname, 'src', 'examples', 'template.ejs'),
            publicPath: webPrefix,
        }),
        new HtmlWebpackPlugin({
            filename: 'sparqlNoStats.html',
            title: 'Ontodia SparQL Demo',
            chunks: ['commons', 'sparqlNoStats'],
            template: path.join(__dirname, 'src', 'examples', 'template.ejs'),
            publicPath: webPrefix,
        }),
        new HtmlWebpackPlugin({
            filename: 'sparqlConstruct.html',
            title: 'Ontodia SparQL Construct Demo',
            chunks: ['commons', 'sparqlConstruct'],
            template: path.join(__dirname, 'src', 'examples', 'template.ejs'),
            publicPath: webPrefix,
        }),
        new HtmlWebpackPlugin({
            filename: 'sparqlRDFGraph.html',
            title: 'Ontodia SparQL RDF Graph Demo',
            chunks: ['commons', 'sparqlRDFGraph'],
            template: path.join(__dirname, 'src', 'examples', 'template.ejs'),
            publicPath: webPrefix,
        }),
        new HtmlWebpackPlugin({
            filename: 'styleCustomization.html',
            title: 'Ontodia Style Customization Demo',
            chunks: ['commons', 'styleCustomization', ],
            template: path.join(__dirname, 'src', 'examples', 'template.ejs'),
            publicPath: webPrefix,
        }),
        new CommonsChunkPlugin('commons', 'commons.chunk.js'),
    ],
    output: {
        path: path.join(__dirname, 'dist', 'examples'),
        filename: '[name].bundle.js',
        chunkFilename: '[id].chunk.js',
        publicPath: webPrefix,
    },
    devtool: '#source-map',
    devServer: {
        historyApiFallback: {
            index: webPrefix,
        },
        proxy: {
            [webPrefix + 'sparql-endpoint**']: {
                target: process.env.SPARQL_ENDPOINT,
                pathRewrite: {[webPrefix + 'sparql-endpoint'] : ''},
                changeOrigin: true,
                secure: false,
            },
        },
    },
};
