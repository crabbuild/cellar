import {application} from './application.js';
import {configuredModel} from './provider.js';
import {configuredObserver, configuredServices} from './preview.js';

export default application(configuredModel(), undefined, configuredObserver(), configuredServices());
