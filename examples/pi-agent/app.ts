import {application} from './application.js';
import {configuredProvider} from './provider.js';
import {configuredObserver} from './preview.js';
export default application(configuredProvider(), undefined, configuredObserver());
